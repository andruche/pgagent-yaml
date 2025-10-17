import argparse

from pgagent_yaml.formatter import Formatter
from pgagent_yaml.pg import Pg


class Statistics:
    def __init__(self, args: argparse.Namespace, pg: Pg):
        self.args = args
        self.pg = pg
        self.formatter = Formatter()

    async def print_stat(self):
        if self.args.step:
            await self.print_step_stat()
        else:
            await self.print_job_stat()

    async def print_job_stat(self):
        job_id = await self.pg.get_job_id_by_name(self.args.job)
        if self.args.start or self.args.till:
            order_col = 'jlgstart'
        else:
            order_col = 'jlgid'
        stat = await self.pg.fetch(
            f'''
            select jlgid as run_id,
                   jlgstatus as status,
                   jlgstart::timestamptz(3)::text as start_time,
                   jlgduration::interval(3)::text as duration,
                   (jlgstart + jlgduration)::timestamptz(3)::text as end_time
              from pgagent.pga_joblog
             where jlgjobid = $1 and
                   (jlgstart >= $2 or $2 is null) and
                   (jlgstart <= $3 or $3 is null)
             order by {order_col} desc
             limit $4
            ''',
            job_id,
            self.args.start,
            self.args.till,
            self.args.limit)
        if self.args.format == 'yaml':
            print(self.formatter.dump(stat))
        else:
            print(self.formatter.render_table(stat))

    async def print_step_stat(self):
        step_id = await self.pg.get_step_id_by_name(
            self.args.job,
            self.args.step
        )
        if self.args.start or self.args.till:
            order_col = 'jslstart'
        else:
            order_col = 'jslid'
        stat = await self.pg.fetch(
            f'''
            select jslid as run_id,
                   jslstatus as status,
                   jslstart::timestamptz(3)::text as start_time,
                   jslduration::interval(3)::text as duration,
                   (jslstart + jslduration)::timestamptz(3)::text as end_time,
                   jslresult as result,
                   jsloutput as output
              from pgagent.pga_jobsteplog
             where jsljstid = $1 and
                   (jslstart >= $2 or $2 is null) and
                   (jslstart <= $3 or $3 is null)
             order by {order_col} desc
             limit $4;
            ''',
            step_id,
            self.args.start,
            self.args.till,
            self.args.limit)
        if self.args.format == 'yaml':
            print(self.formatter.dump(stat))
        else:
            print(self.formatter.render_table(stat))
