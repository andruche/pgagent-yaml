import argparse
import os

import yaml

from .extractor import Extractor, without
from .formatter import Formatter
from .pg import Pg
from .str_diff import color_str_diff


def expand_flags(flags, aliases):
    res = ",".join(
        't' if flags != '-' and (flags == '*' or alias in flags) else 'f'
        for alias in aliases
    )
    res = f'{{{res}}}'
    return res


class Synchronizer:
    job_columns = {
         'id': 'jobid',
         'class': 'jobjclid',
         'name': 'jobname',
         'enabled': 'jobenabled',
         'description': 'jobdesc',
    }
    step_columns = {
        'job_id': 'jstjobid',
        'name': 'jstname',
        'enabled': 'jstenabled',
        'description': 'jstdesc',
        'kind': 'jstkind',
        'on_error': 'jstonerror',
        'connection_string': 'jstconnstr',
        'local_database': 'jstdbname',
        'code': 'jstcode',
    }
    schedule_columns = {
        'job_id': 'jscjobid',
        'name': 'jscname',
        'description': 'jscdesc',
        'enabled': 'jscenabled',
        'start': 'jscstart',
        'end': 'jscend',
        'minutes': 'jscminutes',
        'hours': 'jschours',
        'monthdays': 'jscmonthdays',
        'months': 'jscmonths',
        'weekdays': 'jscweekdays',
    }

    def __init__(self, args: argparse.Namespace, pg: Pg):
        self.args = args
        self.pg = pg
        self.extractor = Extractor(args, pg)
        self.kinds = {
            value: key
            for key, value in self.extractor.kinds.items()
        }
        self.on_errors = {
            value: key
            for key, value in self.extractor.on_errors.items()
        }
        self.formatter = Formatter()
        self.is_dir = os.path.isdir(self.args.source)
        self.job_classes = {}

    def load_jobs(self) -> dict[str, dict]:
        jobs = {}
        if self.is_dir:
            file_names = os.listdir(self.args.source)
        else:
            file_names = [self.args.source]
        for file_name in file_names:
            job = yaml.safe_load(open(os.path.join(self.args.source, file_name)))
            job_name = next(iter(job.keys()))
            jobs[job_name] = job[job_name]
        return jobs

    async def sync(self):
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
        self.job_classes = {
            value: key
            for key, value in self.extractor.job_classes.items()
        }
        diff = self.get_diff(src_jobs, dst_jobs)
        if not diff:
            print('Nothing to do: all jobs are up to date')
            return
        self.print_diff(diff)
        if self.args.yes or self.confirm(len(diff)):
            await self.apply_changes(diff)

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
            queries = []
            queries.extend(self._get_apply_job_queries(job_name, src, dst))
            queries.extend(self._get_apply_table_queries(job_name, src, dst, 'pgagent.pga_jobstep', 'steps'))
            queries.extend(self._get_apply_table_queries(job_name, src, dst, 'pgagent.pga_schedule', 'schedules'))
            async with self.pg.transaction() as con:
                queries = f'--job: {job_name}\n' + '\n'.join(queries)
                self.print_query(queries)
                if not self.args.dry_run:
                    await con.execute(queries)

    def print_query(self, query):
        if not self.args.echo_queries:
            return
        executed = ' (not executed)' if self.args.dry_run else ''
        print(f'\033[33mQUERY{executed}: {query}\033[0m\n')

    @staticmethod
    def _quote_literal(value):
        if value is None:
            return 'null'
        elif isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        elif isinstance(value, (int, bool)):
            return str(value).lower()
        raise TypeError(f'Unknown type for quote value: {value}')

    @staticmethod
    def _get_diff_keys(src, dst):
        return {
            key: value
            for key, value in src.items()
            if value != dst[key]
        }

    def _map_column(self, table, column):
        if table == 'pgagent.pga_job':
            return self.job_columns[column]
        if table == 'pgagent.pga_jobstep':
            return self.step_columns[column]
        if table == 'pgagent.pga_schedule':
            return self.schedule_columns[column]

    def _map_value(self, table, column, value):
        if table == 'pgagent.pga_job':
            if column == 'class':
                return self.job_classes[value]
        if table == 'pgagent.pga_jobstep':
            if column == 'kind':
                return self.kinds[value]
            if column == 'on_error':
                return self.on_errors[value]
        if table == 'pgagent.pga_schedule':
            if column == 'minutes':
                return expand_flags(value, range(60))
            if column == 'hours':
                return expand_flags(value, range(24))
            if column == 'monthdays':
                return expand_flags(value, list(range(1, 32)) + ['last day'])
            if column == 'months':
                return expand_flags(value, range(1, 13))
            if column == 'weekdays':
                return expand_flags(value, [
                    'sunday',
                    'monday',
                    'tuesday',
                    'wednesday',
                    'thursday',
                    'friday',
                    'saturday',
                ])
        return value

    def _map_data(self, table, data):
        return {
            self._map_column(table, key): self._map_value(table, key, value)
            for key, value in data.items()
        }

    def get_job_id_by_name_query(self, job_name):
        table = 'pgagent.pga_job'
        id_column = self._map_column(table, 'id')
        name_column = self._map_column(table, 'name')
        job_name = self._quote_literal(job_name)
        return f'(select {id_column} from {table} where {name_column} = {job_name})'

    def get_job_name_filter(self, table, job_name):
        if job_name:
            return f" and {self._map_column(table, 'job_id')} = {self.get_job_id_by_name_query(job_name)}"
        return ''

    def get_insert_query(self, table, name, data, job_name=None):
        data = self._map_data(table, dict(name=name, **data))
        columns = ', '.join(data.keys())
        values = ', '.join(map(self._quote_literal, data.values()))
        if job_name:
            columns += f", {self._map_column(table, 'job_id')}"
            values += f", {self.get_job_id_by_name_query(job_name)}"
        return f'insert into {table}({columns}) values ({values});'

    def get_update_query(self, table, name, data, job_name=None):
        data = self._map_data(table, data)
        values = ', '.join(
            f'{key} = {self._quote_literal(value)}'
            for key, value in data.items()
        )
        name_column = self._map_column(table, 'name')
        name_value = self._quote_literal(name)
        job_name_filter = self.get_job_name_filter(table, job_name)
        return f'update {table} set {values} where {name_column} = {name_value}{job_name_filter};'

    def get_delete_query(self, table, name, job_name=None):
        name_column = self._map_column(table, 'name')
        name_value = self._quote_literal(name)
        job_name_filter = self.get_job_name_filter(table, job_name)
        return f'delete from {table} where {name_column} = {name_value}{job_name_filter};'

    def _get_apply_job_queries(self, job_name, src, dst):
        if src:
            src = without(src, ('schedules', 'steps'))
        if dst:
            dst = without(dst, ('schedules', 'steps'))

        if src is not None and dst is None:
            return [self.get_insert_query('pgagent.pga_job', job_name, src)]
        if src is not None and dst is not None:
            data = self._get_diff_keys(src, dst)
            if data:
                return [self.get_update_query('pgagent.pga_job', job_name, data)]
        if src is None and dst is not None:
            return [self.get_delete_query('pgagent.pga_job', job_name, src)]
        return []

    def _get_apply_table_queries(self, job_name, src, dst, table, key):
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
                data = self._get_diff_keys(src_item, dst_item)
                if data:
                    res.append(self.get_update_query(table, item_name, data, job_name))
            if src_item is None and dst_item is not None:
                res.append(self.get_delete_query(table, item_name, job_name))
        return res
