from typing import ClassVar
import re

import pandas as pd


class Source:
    """
    A source of information to be loaded into a DataFrame
    """

    def __init__(self, path: str = None):
        self.name = None
        self._path = path

    @property
    def path(self) -> str:
        return self._path or self.name + '.raw'

    def __set_name__(self, _, name):
        self.name = name

    def load(self, path: str) -> pd.DataFrame:
        raise NotImplementedError


class PSQLSource(Source):
    """A data source from ``psql``."""

    def __init__(self, path: str = None, **kwargs):
        super().__init__(path)

        kwargs.setdefault('delimiter', '|')
        kwargs.setdefault('index_col', 0)
        kwargs.setdefault('parse_dates', True)
        self.kwargs = kwargs

    def load(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path, **self.kwargs)


class RegexpSource(Source):
    """Data source to load a file based on a regular expression."""

    syntax: ClassVar[re.Pattern]
    index: ClassVar[str]

    def __init__(self, path: str = None, **kwargs):
        super().__init__(path)
        self.kwargs = kwargs

    def load(self, path: str) -> pd.DataFrame:
        with open(path) as f:
            output = [self.syntax.match(line).groupdict() for line in f]
        df = pd.DataFrame.from_records(output, index=self.index, **self.kwargs)

        df.index = self.coerce(df.index.name, df.index)
        for column_name, series in df.items():
            df[column_name] = self.coerce(column_name, series)
        return df

    def coerce(self, name: str, series: pd.Series) -> pd.Series:
        return series
