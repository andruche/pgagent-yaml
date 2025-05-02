import yaml


class Formatter:
    def __init__(self):
        def str_presenter(dumper, data):
            if '\n' in data:
                return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
            return dumper.represent_scalar('tag:yaml.org,2002:str', data)
        yaml.add_representer(str, str_presenter)

    @staticmethod
    def dump(job: dict, file_name: str = None):
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
