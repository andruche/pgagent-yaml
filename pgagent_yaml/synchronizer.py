import argparse
import os
import sys

import yaml
from pydantic import ValidationError

from .extractor import Extractor
from .data_mapping import YamlToPgMapping, without
from .formatter import Formatter
from .models.job import Job
from .pg import Pg, quote_literal
from .str_diff import color_str_diff


class Synchronizer:
    def __init__(self, args: argparse.Namespace, pg: Pg):
        self.args = args
        self.pg = pg
        self.extractor = Extractor(args, pg)
        self.map = YamlToPgMapping(pg)
        self.formatter = Formatter()
        self.is_dir = os.path.isdir(self.args.source)

    async def sync(self):
        await self.map.load_job_classes()
        src_jobs = self.load_jobs()
        self.args.include_schedule_start_end = any(
            True
            for job_name, job in src_jobs.items()
            if any(
                True
                for schedule in job['schedules']
                if 'start' in schedule
            )
        )
        dst_jobs = await self.extractor.get_jobs()
        diff = self.get_diff(src_jobs, dst_jobs)
        if not diff:
            print('Nothing to do: all jobs are up to date')
            return
        self.print_diff(diff)
        if self.args.yes or self.confirm(len(diff)):
            await self.apply_changes(diff)

    def load_jobs(self) -> dict[str, dict]:
        jobs = {}
        if self.is_dir:
            file_names = (
                os.path.join(self.args.source, file_name)
                for file_name in os.listdir(self.args.source)
            )
        else:
            file_names = [self.args.source]

        for file_name in file_names:
            job = yaml.safe_load(open(file_name))
            job_name = next(iter(job.keys()))
            job_data = job[job_name]
            self.validate_job(file_name, job_name, job_data)
            jobs[job_name] = job_data
        return jobs

    @staticmethod
    def validate_job(file_name, job_name, job_data):
        try:
            Job(**job_data)
        except ValidationError as e:
            print(f'ERROR: cannot load job "{job_name}" from file: {file_name}')
            print(str(e), file=sys.stderr)
            exit(1)

    def get_diff(self, src_jobs, dst_jobs):
        if self.is_dir:
            jobs_names = set(src_jobs.keys()).union(dst_jobs.keys())
        else:
            jobs_names = set(src_jobs.keys())
        res = []
        for job_name in sorted(jobs_names):
            src_job = src_jobs.get(job_name)
            dst_job = dst_jobs.get(job_name)

            if src_job and dst_job:
                # del same data
                for key in list(src_job.keys()):
                    if src_job.get(key) == dst_job.get(key):
                        del src_job[key]
                        del dst_job[key]

            if src_job != dst_job:
                res.append((job_name, src_job, dst_job))
        return res

    def print_diff(self, diff):
        for job_name, src, dst in diff:
            print(
                color_str_diff(
                    self.formatter.dump({job_name: dst} if dst else None),
                    self.formatter.dump({job_name: src} if src else None),
                )
            )

    @staticmethod
    def confirm(changed_jobs_count):
        result = input(f"Are you sure you want to change {changed_jobs_count} jobs? (y/n): ")
        return result == 'y'

    async def apply_changes(self, diff):
        for job_name, src, dst in diff:
            queries = [f'--job: {job_name}']
            queries.extend(self.get_apply_job_queries(job_name, src, dst))
            queries.extend(self.get_apply_table_queries(job_name, src, dst, 'pgagent.pga_jobstep', 'steps'))
            queries.extend(self.get_apply_table_queries(job_name, src, dst, 'pgagent.pga_schedule', 'schedules'))
            queries = '\n'.join(queries)
            self.print_query(queries)
            if not self.args.dry_run:
                await self.pg.execute(queries)

    def print_query(self, query):
        if not self.args.echo_queries:
            return
        executed = ' (not executed)' if self.args.dry_run else ''
        print(f'\033[33mQUERY{executed}: {query}\033[0m\n')

    def get_job_id_by_name_query(self, job_name):
        table = 'pgagent.pga_job'
        id_column = self.map.map_column(table, 'id')
        name_column = self.map.map_column(table, 'name')
        job_name = quote_literal(job_name)
        return f'(select {id_column} from {table} where {name_column} = {job_name})'

    def get_job_name_filter(self, table, job_name):
        if job_name:
            column = self.map.map_column(table, 'job_id')
            subquery = self.get_job_id_by_name_query(job_name)
            return f" and {column} = {subquery}"
        return ''

    def get_insert_query(self, table, name, data, job_name=None):
        data = self.map.map_table_row(table, dict(name=name, **data))
        columns = ', '.join(data.keys())
        values = ', '.join(map(quote_literal, data.values()))
        if job_name:
            columns += f", {self.map.map_column(table, 'job_id')}"
            values += f", {self.get_job_id_by_name_query(job_name)}"
        return f'insert into {table}({columns}) values ({values});'

    def get_update_query(self, table, name, data, job_name=None):
        data = self.map.map_table_row(table, data)
        values = ', '.join(
            f'{key} = {quote_literal(value)}'
            for key, value in data.items()
        )
        name_column = self.map.map_column(table, 'name')
        name_value = quote_literal(name)
        job_name_filter = self.get_job_name_filter(table, job_name)
        return f'update {table} set {values} where {name_column} = {name_value}{job_name_filter};'

    def get_delete_query(self, table, name, job_name=None):
        name_column = self.map.map_column(table, 'name')
        name_value = quote_literal(name)
        job_name_filter = self.get_job_name_filter(table, job_name)
        return f'delete from {table} where {name_column} = {name_value}{job_name_filter};'

    def get_apply_job_queries(self, job_name, src, dst):
        if src:
            src = without(src, ('schedules', 'steps'))
        if dst:
            dst = without(dst, ('schedules', 'steps'))

        if src is not None and dst is None:
            return [self.get_insert_query('pgagent.pga_job', job_name, src)]
        if src is not None and dst is not None:
            data = self.get_diff_keys(src, dst)
            if data:
                return [self.get_update_query('pgagent.pga_job', job_name, data)]
        if src is None and dst is not None:
            return [self.get_delete_query('pgagent.pga_job', job_name, src)]
        return []

    def get_apply_table_queries(self, job_name, src, dst, table, key):
        if src is None:
            return []
        src_items = (src or {}).get(key, {})
        dst_items = (dst or {}).get(key, {})
        if not src_items and not dst_items:
            return []
        res = []
        item_names = set(src_items.keys()).union(dst_items.keys())
        for item_name in item_names:
            src_item = src_items.get(item_name)
            dst_item = dst_items.get(item_name)
            if src_item is not None and dst_item is None:
                res.append(self.get_insert_query(table, item_name, src_item, job_name))
            if src_item is not None and dst_item is not None:
                data = self.get_diff_keys(src_item, dst_item)
                if data:
                    res.append(self.get_update_query(table, item_name, data, job_name))
            if src_item is None and dst_item is not None:
                res.append(self.get_delete_query(table, item_name, job_name))
        return res

    @staticmethod
    def get_diff_keys(src, dst):
        return {
            key: value
            for key, value in src.items()
            if value != dst[key]
        }
