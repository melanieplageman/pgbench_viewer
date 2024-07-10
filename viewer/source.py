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
            'index_col': 0,
            'parse_dates': True,
            **self._kwargs,
        }
        return pd.read_csv(path, **kwargs)


class MultiPSQLSource(Source):
    def __init__(self, prefix: str = None, path: str = None,
                 pivot_col: list[str] = None, plot_values: list[str] = None,
                 **kwargs):
        self.pivot_col = pivot_col
        self.plot_values = plot_values
        self._prefix = prefix
        self._kwargs = kwargs
        self._path = path

    def load(self, path: str) -> pd.DataFrame:
        assert isinstance(self.pivot_col, list)
        kwargs = {
            'delimiter': '|',
            'index_col': 0,
            'parse_dates': True,
        }
        df = pd.read_csv(path, **kwargs)

        df = df.pivot(columns = self.pivot_col,
                        values = self.plot_values)

        df.columns = df.columns.to_flat_index().map(lambda x: '_'.join(x))
        return df



class RegexpSource(Source):
    """Data source to load a file based on a regular expression."""

    syntax: ClassVar[re.Pattern]
    index: ClassVar[str]

    def load(self, path: str) -> pd.DataFrame:
        with open(path) as f:
            output = [self.syntax.match(line).groupdict() for line in f]
        kwargs = {'index': self.index, **self._kwargs}
        df = pd.DataFrame.from_records(output, **kwargs)

        df.index = self.coerce_index(df.index)
        for column_name, series in df.items():
            df[column_name] = self.coerce(column_name, series)
        return df

    def coerce_index(self, series: pd.Series) -> pd.Series:
        return series

    def coerce(self, name: str, series: pd.Series) -> pd.Series:
        return series
