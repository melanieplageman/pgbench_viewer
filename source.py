import json
from glob import glob
import os
import pandas as pd
import re
import numpy as np

from viewer.source import *


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
        return pd.to_numeric(series, errors='coerce')


class ExecutionReportsSource(Source):
    header = [
        'client_id', 'transaction_no', 'time', 'script_no', 'time_epoch',
        'time_us', 'schedule_lag', 'retries',
    ]

    cache_name = 'pickle'
    percentile_limit = 0.001
    n = 10
    interval = '1s'

    def __init__(self, *args, path='execution_reports', **kwargs):
        super().__init__(*args, path=path, **kwargs)

    def cache_path(self, path):
        """Return the path of the cache file."""

        cache_filename = "-".join([
            self.cache_name,
            str(self.percentile_limit),
            str(self.n),
            self.interval,
        ])
        return os.path.join(path, cache_filename)

    def iterreports(self, path):
        return glob(os.path.join(path, 'pgbench_log*'))

    def load(self, path):
        try:
            mtime = os.path.getmtime(self.cache_path(path))
        except OSError:
            print("No cached execution reports. Loading data now.")
            return self._get_cache(path)

        if any(os.path.getmtime(name) >= mtime for name in self.iterreports(path)):
            return self._get_cache(path)

        return pd.read_pickle(self.cache_path(path))

    def _get_cache(self, path):
        output = []
        for name in self.iterreports(path):
            print(f"processing {name}")
            df = pd.read_table(
                name, delimiter=' ', names=self.header, memory_map=True,
            )
            output.append(df)

        df = pd.concat(output)

        # Create the time series index
        df.index = pd.to_datetime(
            df['time_epoch'] * 1_000_000 + df['time_us'], unit='us', utc=True,
        )
        df.sort_index(inplace=True)

        resample = df.resample(self.interval)

        quantiles = np.linspace(0 + self.percentile_limit,
                                1 - self.percentile_limit,
                                num=self.n * 2 + 1)

        df = resample['time'].quantile(quantiles).unstack()
        df['mean'] = resample['time'].mean()

        df.to_pickle(os.path.join(path, self.cache_path(path)))
        return df
