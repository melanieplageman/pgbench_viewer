from __future__ import annotations

import pandas as pd
import json
import os
import re

from viewer.source import *
from viewer.loader import *


class MainJSONSource(Source):
    def load(self, path: str) -> pd.DataFrame:
        with open(path) as f:
            data = json.load(f)

        df = pd.DataFrame.from_records(data['data']['meminfo'], index='ts')
        df.index = pd.to_datetime(df.index, format='%Y-%m-%dT%H:%M:%S,%f%z')
        meminfo_df = df

        df = pd.DataFrame.from_records(data['data']['pidstat'][0]['data'], index='ts')
        df.index = pd.to_datetime(df.index, unit='s', utc=True)
        pidstat_checkpointer_df = df

        return meminfo_df.join(pidstat_checkpointer_df, how='outer')


class IOStatSource(Source):
    """Data source to load an ``iostat`` JSON file."""

    def load(self, path: str) -> pd.DataFrame:
        with open(path) as f:
            data = json.load(f)

        timeline = []
        output = []
        for item in data['sysstat']['hosts'][0]['statistics']:
            timeline.append(item['timestamp'])

            output_item = {}
            for name, number in item['avg-cpu'].items():
                output_item[name] = number
            for name, number in item['disk'][0].items():
                output_item[name] = number
            output.append(output_item)

        df = pd.DataFrame.from_records(output)
        df.index = pd.to_datetime(timeline)
        return df


class PGBenchRunProgressSource(Source):
    """Data source to load a ``pgbench`` progress file."""

    # progress: 560.0 s, 55376.5 tps, lat 0.866 ms stddev 0.268, 0 failed
    syntax = re.compile(
        r"progress: (?P<ts>\d+\.\d+) s, "
        r"(?P<tps>\d+\.\d+) tps, "
        r"lat (?P<lat>\d+\.\d+) ms stddev (?P<lat_stddev>\d+\.\d+|NaN), "
        r"(?P<failed>\d+) failed"
    )

    def load(self, path: str) -> pd.DataFrame:
        with open(path) as f:
            output = [self.syntax.match(line).groups() for line in f]

        header = ['ts', 'tps', 'lat', 'stddev', 'failed']
        df = pd.DataFrame.from_records(output, index='ts', columns=header)

        df.index = pd.to_datetime(pd.to_numeric(df.index), unit='s', utc=True)
        for column_name, series in df.items():
            df[column_name] = pd.to_numeric(series)
        return df


class TestLoader(Loader):
    buffercache_progress = PSQLSource()
    relfrozenxid_progress = PSQLSource()
    aggwaits = PSQLSource()
    pg_stat_wal_progress = PSQLSource(parse_dates=['ts', 'stats_reset'])
    # pg_stat_io_progress = PSQLSource(parse_dates=['ts', 'stats_reset'])

    pgbench_run_progress = PGBenchRunProgressSource('pgbench_run_progress.raw')

    main = MainJSONSource('results/e921166b-219e-4912-b165-d133e3838e5e.json')
    iostat = IOStatSource('iostat.raw')


df = TestLoader.load('/mnt/force/pgresults/A/tmp.84G0vTOri4')

print(df)
print("Index:", df.index.dtype)
print(df.dtypes)
