from typing import ClassVar
import os
import pandas as pd

from viewer.source import Source


class Loader:
    __schema__: ClassVar[dict[str, Source]] = {}

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)

        # Duplicate cls.__schema__
        cls.__schema__ = {**cls.__schema__}

        for name, source in vars(cls).items():
            if name.startswith('_'):
                continue
            if not isinstance(source, Source):
                continue
            cls.__schema__[name] = source

    @classmethod
    def load(cls, root: str) -> pd.DataFrame:
        """
        Invoke all configured sources against the directory *root*.
        """

        output_df = pd.DataFrame()
        for source in cls.__schema__.values():
            source_df = source.load(source.path(root))
            source_df = source_df.convert_dtypes()
            output_df = output_df.join(source_df, how='outer')
        return output_df
