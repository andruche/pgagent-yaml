import os
import argparse
import asyncio
import shutil

from .extractor import Extractor
from .synchronizer import Synchronizer
from .pg import Pg
from . import __version__

node_format = 'migration_path -> [user@]host:port/database'


async def run(args):
    if args.command == 'export':
        pg = Pg(args)
        await pg.init()
        await Extractor(args, pg).export()

    elif args.command == 'sync':
        pg = Pg(args)
        await pg.init()
        await Synchronizer(args, pg).sync()

    else:
        raise Exception(f'unknown command {args.command}')


def main():
    def add_connection_args(parser):
        parser.add_argument('-d', '--dbname',
                            type=str, help='database name to connect to')
        parser.add_argument('-h', '--host',
                            type=str, help='database server host or socket directory')
        parser.add_argument('-p', '--port',
                            type=str, help='database server port')
        parser.add_argument('-U', '--user',
                            type=str, help='database user name')
        parser.add_argument('-W', '--password',
                            type=str, help='database user password')

    arg_parser = argparse.ArgumentParser(
        epilog='Report bugs: https://gitlab.uis.dev/pg_tools/pgagent-yaml/issues',
        conflict_handler='resolve',
    )

    arg_parser.add_argument(
        '--version',
        action='version',
        version=__version__
    )

    subparsers = arg_parser.add_subparsers(
        dest='command',
        title='commands'
    )

    parser_export = subparsers.add_parser(
        'export',
        help='export pgagent jobs to files',
        conflict_handler='resolve',
    )
    add_connection_args(parser_export)
    parser_export.add_argument(
        '--out-dir',
        required=True,
        help='directory for exporting files'
    )
    parser_export.add_argument(
        '--clean',
        action="store_true",
        help='clean out_dir if not empty '
        '(env variable PGAGENT_YAML_AUTOCLEAN=true)'
    )
    parser_export.add_argument(
        '--ignore-version',
        action="store_true",
        help='try exporting an unsupported server version'
    )
    parser_export.add_argument(
        '--include-schedule-start-end',
        action="store_true",
        help='include "start", "end" fields (without by default)'
    )

    parser_sync = subparsers.add_parser(
        'sync',
        help='sync files to pgagent jobs',
        conflict_handler='resolve',
    )
    add_connection_args(parser_sync)
    parser_sync.add_argument(
        '--source',
        required=True,
        help='directory or file with jobs to sync to pgagent'
    )
    parser_sync.add_argument(
        '--dry-run',
        action="store_true",
        help='test run without real changes'
    )
    parser_sync.add_argument(
        '--echo-queries',
        action="store_true",
        help='echo commands sent to server'
    )
    parser_sync.add_argument(
        '-y', '--yes',
        action="store_true",
        help='do not ask confirm'
    )
    parser_sync.add_argument(
        '--ignore-version',
        action="store_true",
        help='try exporting an unsupported server version'
    )

    args = arg_parser.parse_args()

    if args.command == 'export':
        if os.path.exists(args.out_dir) and os.listdir(args.out_dir):
            if args.clean or os.environ.get('PGAGENT_YAML_AUTOCLEAN') == 'true':
                shutil.rmtree(args.out_dir)
            else:
                parser_export.error('out_dir directory not empty '
                                    '(you can use option --clean)')
        try:
            os.makedirs(args.out_dir, exist_ok=True)
        except Exception:
            arg_parser.error("can not access to directory '%s'" % args.out_dir)

    if args.command == 'sync':
        if not os.path.exists(args.source):
            parser_export.error(f'file or directory not found: {args.source}')

    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run(args))
