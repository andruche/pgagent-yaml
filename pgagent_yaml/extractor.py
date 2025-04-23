import argparse
import os
import sys

import yaml

from .pg import Pg


def without(_dict, _key):
    _dict = dict(_dict)
    if isinstance(_key, str):
        del _dict[_key]
    else:
        for __key in _key:
            del _dict[__key]
    return _dict


def compact_flags(flags, aliases):
    if all(flags):
        return '*'
    if not any(flags):
        return '-'
    return [
        alias
        for flag, alias in zip(flags, aliases)
        if flag
    ]


class Extractor:
    on_errors = {
        's': 'success',
        'f': 'fail',
        'i': 'ignore',
    }
    kinds = {
        's': 'sql',
        'b': 'batch'
    }

    def __init__(self, args: argparse.Namespace, pg: Pg):
        self.args = args
        self.pg = pg

        def str_presenter(dumper, data):
            if '\n' in data:
                return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
            return dumper.represent_scalar('tag:yaml.org,2002:str', data)
        yaml.add_representer(str, str_presenter)

    async def export(self):
        jobs = await self.get_jobs()
        for job in jobs:
            file_name = os.path.join(self.args.out_dir, f"{job['name']}.yaml")
            with open(file_name, 'w') as file:
                yaml.dump(
                    {job.pop('name'): job},
                    file,
                    allow_unicode=True,
                    sort_keys=False,
                    width=float('inf')
                )

    async def get_jobs(self) -> list[dict]:
        jobs = await self._get_jobs_data()
        steps = await self._get_steps_data()
        schedules = await self._get_schedules_data()
        jobs = {
            job['id']: dict(without(job, 'id'), schedules={}, steps={})
            for job in jobs
        }
        await self.check_schedule_start_end(jobs, schedules)
        if self.args.include_schedule_start_end:
            del_keys = ('job_id', 'name')
        else:
            del_keys = ('job_id', 'name', 'start', 'end')
        for schedule in schedules:
            jobs[schedule['job_id']]['schedules'][schedule['name']] = without(schedule, del_keys)
        for step in steps:
            jobs[step['job_id']]['steps'][step['name']] = without(step, ('job_id', 'name'))
        return list(jobs.values())

    async def _get_jobs_data(self):
        return await self.pg.fetch('''
            select jobid as id,
                   jobname as name,
                   jobenabled as enabled,
                   jobdesc as description
              from pgagent.pga_job;
        ''')

    async def _get_steps_data(self):
        return [
            dict(
                step,
                kind=self.kinds[step['kind']],
                on_error=self.on_errors[step['on_error']],
            )
            for step in await self.pg.fetch('''
                select jstjobid as job_id,
                       jstname as name,
                       jstenabled as enabled,
                       jstdesc as description,
                       jstkind as kind,
                       jstonerror as on_error,
                       jstconnstr as connection_string,
                       jstdbname as local_database,
                       jstcode as code
                  from pgagent.pga_jobstep
                 order by jstname;
            ''')
        ]

    async def _get_schedules_data(self):
        return [
            dict(
                schedule,
                minutes=compact_flags(schedule['minutes'], range(60)),
                hours=compact_flags(schedule['hours'], range(24)),
                monthdays=compact_flags(schedule['monthdays'], range(1, 32)),
                months=compact_flags(schedule['months'], range(1, 13)),
                weekdays=compact_flags(schedule['weekdays'], [
                    'sunday',
                    'monday',
                    'tuesday',
                    'wednesday',
                    'thursday',
                    'friday',
                    'saturday',
                ])
            )
            for schedule in await self.pg.fetch('''
                select jscjobid as job_id,
                       jscname as name,
                       jscdesc as description,
                       jscenabled as enabled,
                       jscstart as start,
                       jscend as end,
                       jscminutes as minutes,
                       jschours as hours,
                       jscmonthdays as monthdays,
                       jscmonths as months,
                       jscweekdays as weekdays
                  from pgagent.pga_schedule
                 order by jscname;
            ''')
        ]

    async def check_schedule_start_end(self, jobs, schedules):
        if self.args.include_schedule_start_end:
            return

        now = (await self.pg.fetch('select now()'))[0]['now']
        has_warning = False
        for schedule in schedules:
            job_name = jobs[schedule["job_id"]]["name"]
            schedule_name = f'{job_name}/{schedule["name"]}'
            if schedule['start'] and schedule['start'] > now:
                print(
                    f'WARNING: The schedule "{schedule_name}" is inactive '
                    f'(start="{schedule["start"]}" > now)',
                    file=sys.stderr
                )
                has_warning = True
            if schedule['end'] and schedule['end'] < now:
                print(
                    f'WARNING: The schedule "{schedule_name}" is inactive '
                    f'(end="{schedule["end"]}" < now)',
                    file=sys.stderr
                )
                has_warning = True
        if has_warning:
            print(
                'HINT: Use --include-schedule-start-end for export schedules with "start", "end" fields',
                file=sys.stderr
            )
