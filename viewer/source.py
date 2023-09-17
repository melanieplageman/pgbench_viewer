from typing import ClassVar
import os
import re

import pandas as pd


class Source:
    """
    A source of information to be loaded into a DataFrame
    """

    name: str

    def __set_name__(self, _, name: str):
        self.name = name

    def __init__(self, prefix: str = None, path: str = None, **kwargs):
        self._prefix = prefix
        self._path = path
        self._kwargs = kwargs

    @property
    def prefix(self) -> str:
        return self._prefix or self.name

    def path(self, root: str) -> str:
        return os.path.join(root, self._path or self.name + '.raw')

    def load(self, path: str) -> pd.DataFrame:
        raise NotImplementedError


class PSQLSource(Source):
    """A data source from ``psql``."""

    def load(self, path: str) -> pd.DataFrame:
        kwargs = {
            'delimiter': '|',
            'index_col': 0,
            'parse_dates': True,
            **self._kwargs,
        }
        return pd.read_csv(path, **kwargs)


class RegexpSource(Source):
    """Data source to load a file based on a regular expression."""

    syntax: ClassVar[re.Pattern]
    index: ClassVar[str]

    def load(self, path: str) -> pd.DataFrame:
        with open(path) as f:
            output = [self.syntax.match(line).groupdict() for line in f]
        kwargs = {'index': self.index, **self._kwargs}
        df = pd.DataFrame.from_records(output, **kwargs)

        df.index = self.coerce(df.index.name, df.index)
        for column_name, series in df.items():
            df[column_name] = self.coerce(column_name, series)
        return df

    def coerce(self, name: str, series: pd.Series) -> pd.Series:
        return series
