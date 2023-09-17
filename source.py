import json
import os
import pandas as pd
import re

from viewer.source import Source, PSQLSource, RegexpSource


class MainJSONSource(Source):
    def __init__(self, *args, path: str = 'results', **kwargs):
        super().__init__(*args, path=path, **kwargs)

    def path(self, root: str) -> str:
        path = super().path(root)
        return os.path.join(path, os.listdir(path)[0])

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


class PGBenchRunProgressSource(RegexpSource):
    """Data source to load a ``pgbench`` progress file."""

    # progress: 560.0 s, 55376.5 tps, lat 0.866 ms stddev 0.268, 0 failed
    syntax = re.compile(
        r"progress: (?P<ts>\d+\.\d+) s, "
        r"(?P<tps>\d+\.\d+) tps, "
        r"lat (?P<lat>\d+\.\d+) ms stddev (?P<lat_stddev>\d+\.\d+|NaN), "
        r"(?P<failed>\d+) failed"
    )
    index = 'ts'

    def coerce_index(self, series: pd.Series) -> pd.Series:
        return pd.to_datetime(pd.to_numeric(series), unit='s', utc=True)

    def coerce(self, name: str, series: pd.Series) -> pd.Series:
        return pd.to_numeric(series)
