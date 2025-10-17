from typing import Any

import yaml


class Formatter:
    def __init__(self):
        def str_presenter(dumper, data):
            if '\n' in data:
                return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
            return dumper.represent_scalar('tag:yaml.org,2002:str', data)
        yaml.add_representer(str, str_presenter)

    @staticmethod
    def dump(job: Any, file_name: str = None):
        if job is None:
            return ''
        file = None
        if file_name:
            file = open(file_name, 'w')
        return yaml.dump(
            job,
            file,
            allow_unicode=True,
            sort_keys=False,
            width=float('inf')
        )

    @staticmethod
    def render_table(data):
        def format_val(col, val, row_number, col_number):
            if row_number == 0:  # header
                res = str(val).center(col_width[col])
            else:  # rows
                res = str(val or '').ljust(col_width[col])
            if col_number == len(row):  # last column
                res = res.rstrip()
            return res

        if not data:
            return ''

        col_width = {}
        header = {col: col for col in data[0]}
        data = [header] + data
        for row in data:
            for col, val in row.items():
                col_width[col] = max(col_width.get(col, 0), len(str(val or '')))

        return '\n'.join(
            ' | '.join(
                format_val(col, val, row_number, col_number)
                for col_number, (col, val) in enumerate(row.items(), 1)
            )
            for row_number, row in enumerate(data)
        )
