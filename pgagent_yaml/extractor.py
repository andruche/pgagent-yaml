import argparse
import os
import sys

from .formatter import Formatter
from .pg import Pg
from .data_mapping import PgToYamlMapping


class Extractor:
    def __init__(self, args: argparse.Namespace, pg: Pg):
        self.args = args
        self.pg = pg
        self.map = PgToYamlMapping(pg)
        self.formatter = Formatter()

    async def export(self) -> None:
        jobs = await self.get_jobs()
        for job_name, job in jobs.items():
            file_name = os.path.join(self.args.out_dir, f"{job_name}.yaml")
            self.formatter.dump({job_name: job}, file_name)

    async def get_jobs(self) -> dict:
        await self.map.load_job_classes()
        jobs = self.map.map_data(
            await self.get_jobs_data(),
            await self.get_schedules_data(),
            await self.get_steps_data(),
        )
        if not self.args.include_schedule_start_end:
            self.del_schedules_start_end(jobs)
        return jobs

    async def get_jobs_data(self) -> list[dict]:
        return await self.pg.fetch('''
            select jobid,
                   jobjclid,
                   jobname,
                   jobenabled,
                   jobdesc
              from pgagent.pga_job j
        ''')

    async def get_steps_data(self) -> list[dict]:
        return await self.pg.fetch('''
            select jstjobid,
                   jstname,
                   jstenabled,
                   jstdesc,
                   jstkind,
                   jstonerror,
                   jstconnstr,
                   jstdbname,
                   jstcode
              from pgagent.pga_jobstep
             order by jstname;
        ''')

    async def get_schedules_data(self) -> list[dict]:
        return await self.pg.fetch('''
            select jscjobid,
                   jscname,
                   jscdesc,
                   jscenabled,
                   jscstart,
                   jscend,
                   jscminutes,
                   jschours,
                   jscmonthdays,
                   jscmonths,
                   jscweekdays
              from pgagent.pga_schedule
             order by jscname;
        ''')

    def del_schedules_start_end(self, jobs: dict[str, dict]) -> None:
        has_warning = False
        for job_name, job in jobs.items():
            for schedule_name, schedule in job['schedules'].items():
                has_warning = self.check_schedules_start_end(job_name, schedule_name, schedule) or has_warning
                del schedule['start']
                del schedule['end']
        if has_warning:
            print(
                'HINT: Use --include-schedule-start-end for export schedules with "start", "end" fields',
                file=sys.stderr
            )

    def check_schedules_start_end(self, job_name: str, schedule_name: str, schedule: dict) -> bool:
        schedule_name = f'{job_name}/{schedule_name}'
        header = f'WARNING: The schedule "{schedule_name}" is inactive'
        has_warning = False
        if schedule['start'] > self.pg.now:
            print(f'{header} (start="{schedule["start"]}" > now)', file=sys.stderr)
            has_warning = True
        if schedule['end'] and schedule['end'] < self.pg.now:
            print(f'{header} (end="{schedule["end"]}" < now)', file=sys.stderr)
            has_warning = True
        return has_warning
