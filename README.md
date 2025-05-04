## pgagent-yaml - Yaml interface for pgagent

Export structure of pgagent jobs to yaml files
Sync yaml files to pgagent jobs

## installation

```
pip install pgagent-yaml
```

## usage

### export jobs:
```
usage: pgagent_yaml export [--help] [-d DBNAME] [-h HOST] [-p PORT] [-U USER] [-W PASSWORD] --out-dir OUT_DIR [--clean]
                           [--ignore-version] [--include-schedule-start-end]

options:
  --help                show this help message and exit
  -d DBNAME, --dbname DBNAME
                        database name to connect to
  -h HOST, --host HOST  database server host or socket directory
  -p PORT, --port PORT  database server port
  -U USER, --user USER  database user name
  -W PASSWORD, --password PASSWORD
                        database user password
  --out-dir OUT_DIR     directory for exporting files
  --clean               clean out_dir if not empty (env variable PGAGENT_YAML_AUTOCLEAN=true)
  --ignore-version      try exporting an unsupported server version
  --include-schedule-start-end
                        include "start", "end" fields (without by default)
```

### sync jobs:
```
usage: pgagent_yaml sync [--help] [-d DBNAME] [-h HOST] [-p PORT] [-U USER] [-W PASSWORD] --source SOURCE [--dry-run]
                         [--echo-queries] [-y] [--ignore-version]

options:
  --help                show this help message and exit
  -d DBNAME, --dbname DBNAME
                        database name to connect to
  -h HOST, --host HOST  database server host or socket directory
  -p PORT, --port PORT  database server port
  -U USER, --user USER  database user name
  -W PASSWORD, --password PASSWORD
                        database user password
  --source SOURCE       directory or file with jobs to sync to pgagent
  --dry-run             test run without real changes
  --echo-queries        echo commands sent to server
  -y, --yes             do not ask confirm
  --ignore-version      try exporting an unsupported server version
```

## examples

```
$ pgagent_yaml export -d my_database -h 127.0.0.1 -p 5432 -U postgres --out-dir /tmp/jobs/
$ pgagent_yaml sync -d my_database -h 127.0.0.1 -p 5432 -U postgres --source /tmp/jobs/
```
