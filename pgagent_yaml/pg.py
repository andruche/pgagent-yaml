import datetime
import sys

import asyncpg


def quote_literal(value):
    if value is None:
        return 'null'
    elif isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    elif isinstance(value, (int, bool)):
        return str(value).lower()
    raise TypeError(f'Unknown type for quote value: {value}')


class Pg:
    con: asyncpg.Connection
    compatible_version = {
        'from': (3, 2),
        'to': (4, 2)
    }
    now: datetime.datetime

    def __init__(self, args):
        self.args = args

    async def init(self):
        self.con = await asyncpg.connect(
            database=self.args.dbname,
            user=self.args.user,
            password=self.args.password,
            host=self.args.host,
            port=self.args.port,
            statement_cache_size=0,
        )
        await self.check_version()
        self.now = (await self.fetch('select now()'))[0]['now']

    async def fetch(self, query: str, **params) -> list[dict]:
        rows = await self.con.fetch(query, *params)
        return [dict(row) for row in rows]

    async def execute(self, query: str, **params) -> None:
        await self.con.execute(query, *params)

    async def get_pgagent_version(self) -> tuple[int, int]:
        ver = await self.fetch('''
            select extversion as version
              from pg_extension 
             where extname = 'pgagent';
        ''')
        if not ver:
            raise Exception('extension pgagent does not installed')
        ver = ver[0]['version'].split('.')
        return int(ver[0]), int(ver[1])

    async def check_version(self) -> None:
        if self.args.ignore_version:
            return

        ver = await self.get_pgagent_version()
        ver_from = self.compatible_version['from']
        ver_to = self.compatible_version['to']
        if not (ver_from <= ver <= ver_to):
            def to_str(_ver):
                return ".".join(map(str, _ver))
            print(
                f'pgagent {to_str(ver)} not supported (only {to_str(ver_from)} - {to_str(ver_to)})'
                f', use --ignore-version to run anyway',
                file=sys.stderr
            )
            exit(1)
