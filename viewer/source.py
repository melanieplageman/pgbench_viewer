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
        return self._path or self.name

    def __set_name__(self, _, name):
        self.name = name

    def load(self, path: str) -> pd.DataFrame:
        raise NotImplementedError


class PSQLSource(Source):
    def __init__(
        self, *args, delimiter='|', index_col='ts', parse_dates=True, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.delimiter = delimiter
        self.index_col = index_col
        self.parse_dates = parse_dates

    @property
    def path(self) -> str:
        return self._path or self.name + '.raw'

    def load(self, path: str) -> pd.DataFrame:
        return pd.read_csv(
            path,
            delimiter=self.delimiter,
            index_col=self.index_col,
            parse_dates=self.parse_dates,
        )
