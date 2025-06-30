import argparse
from .pg import Pg


class Runner:
    def __init__(self, args: argparse.Namespace, pg: Pg):
        self.args = args
        self.pg = pg

    async def run_now(self):
        res = await self.pg.fetch(
            '''
            update pgagent.pga_job
               set jobnextrun = now()
             where jobname = $1
            returning jobname
            ''',
            self.args.job
        )
        if not res:
            raise Exception(f'job "{self.args.job}" not found')
