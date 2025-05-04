from .pg import Pg


def switch_key_value(data):
    return {
        value: key
        for key, value in data.items()
    }


def without(row, key):
    row = dict(row)
    if isinstance(key, str):
        if key in row:
            del row[key]
    else:
        for _key in key:
            if _key in row:
                del row[_key]
    return row


job_classes_query = '''
    select jclid as id,
           jclname as name
      from pgagent.pga_jobclass
'''


class PgToYamlMapping:
    job_columns = {
        'jobid': 'id',
        'jobjclid': 'class',
        'jobname': 'name',
        'jobenabled': 'enabled',
        'jobdesc': 'description',
    }
    step_columns = {
        'jstjobid': 'job_id',
        'jstname': 'name',
        'jstenabled': 'enabled',
        'jstdesc': 'description',
        'jstkind': 'kind',
        'jstonerror': 'on_error',
        'jstconnstr': 'connection_string',
        'jstdbname': 'local_database',
        'jstcode': 'code',
    }
    schedule_columns = {
        'jscjobid': 'job_id',
        'jscname': 'name',
        'jscdesc': 'description',
        'jscenabled': 'enabled',
        'jscstart': 'start',
        'jscend': 'end',
        'jscminutes': 'minutes',
        'jschours': 'hours',
        'jscmonthdays': 'monthdays',
        'jscmonths': 'months',
        'jscweekdays': 'weekdays',
    }
    step_on_errors = {
        's': 'success',
        'f': 'fail',
        'i': 'ignore',
    }
    step_kinds = {
        's': 'sql',
        'b': 'batch'
    }
    job_classes: dict[int, str]

    def __init__(self, pg: Pg):
        self.pg = pg

    async def load_job_classes(self):
        self.job_classes = {
            row['id']: row['name']
            for row in await self.pg.fetch(job_classes_query)
        }

    def map_column(self, table, column):
        if table == 'pgagent.pga_job':
            return self.job_columns[column]
        if table == 'pgagent.pga_jobstep':
            return self.step_columns[column]
        if table == 'pgagent.pga_schedule':
            return self.schedule_columns[column]

    def map_value(self, table, column, value):
        columns = (column, self.map_column(table, column))
        if table == 'pgagent.pga_job':
            if 'class' in columns:
                return self.job_classes[value]
        if table == 'pgagent.pga_jobstep':
            if 'kind' in columns:
                return self.step_kinds[value]
            if 'on_error' in columns:
                return self.step_on_errors[value]
        if table == 'pgagent.pga_schedule':
            if 'minutes' in columns:
                return self.map_flags(value, range(60))
            if 'hours' in columns:
                return self.map_flags(value, range(24))
            if 'monthdays' in columns:
                return self.map_flags(value, list(range(1, 32)) + ['last day'])
            if 'months' in columns:
                return self.map_flags(value, range(1, 13))
            if 'weekdays' in columns:
                return self.map_flags(value, [
                    'sunday',
                    'monday',
                    'tuesday',
                    'wednesday',
                    'thursday',
                    'friday',
                    'saturday',
                ])
        return value

    def map_table_row(self, table, row):
        return {
            self.map_column(table, key): self.map_value(table, key, value)
            for key, value in row.items()
        }

    def map_table(self, table, rows):
        return [
            self.map_table_row(table, row)
            for row in rows
        ]

    def map_data(self, jobs, schedules, steps):
        jobs = self.map_table('pgagent.pga_job', jobs)
        schedules = self.map_table('pgagent.pga_schedule', schedules)
        steps = self.map_table('pgagent.pga_jobstep', steps)
        jobs = {
            job['id']: dict(without(job, 'id'), schedules={}, steps={})
            for job in jobs
        }
        for schedule in schedules:
            jobs[schedule['job_id']]['schedules'][schedule['name']] = without(schedule, ('job_id', 'name'))
        for step in steps:
            jobs[step['job_id']]['steps'][step['name']] = without(step, ('job_id', 'name'))
        return {
            job.pop('name'): job
            for job in jobs.values()
        }

    @staticmethod
    def map_flags(flags, aliases):
        if all(flags):
            return '*'
        if not any(flags):
            return '-'
        return [
            alias
            for flag, alias in zip(flags, aliases)
            if flag
        ]


class YamlToPgMapping(PgToYamlMapping):
    job_columns = switch_key_value(PgToYamlMapping.job_columns)
    step_columns = switch_key_value(PgToYamlMapping.step_columns)
    schedule_columns = switch_key_value(PgToYamlMapping.schedule_columns)
    step_on_errors = switch_key_value(PgToYamlMapping.step_on_errors)
    step_kinds = switch_key_value(PgToYamlMapping.step_kinds)
    job_classes: dict[str, int]

    async def load_job_classes(self):
        await super().load_job_classes()
        self.job_classes = switch_key_value(self.job_classes)

    @staticmethod
    def map_flags(flags, aliases):
        res = ",".join(
            't' if flags != '-' and (flags == '*' or alias in flags) else 'f'
            for alias in aliases
        )
        res = f'{{{res}}}'
        return res
