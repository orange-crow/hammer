from pprint import pprint
from typing import Any, Dict, List, Union

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from scipy import stats

from ..schema import TableSchema
from ..utils.schema import init_schema
from .table_base import TableBase


class PandasTable(pd.DataFrame, TableBase):
    _metadata = ["_schema"]  # 保留自定义属性

    def __init__(self, *args, **kwargs):
        schema = kwargs.pop("schema", None)  # 从 kwargs 中取出 schema
        super(PandasTable, self).__init__(*args, **kwargs)
        if schema is not None:
            self._schema = init_schema(schema)
        else:
            self._schema = None

    @classmethod
    def _from_file(
        cls, file_path: str, schema: Union[str, List[Dict], TableSchema], reader: callable, **kwargs
    ) -> "TableBase":
        schema = init_schema(schema)
        df = reader(file_path, **kwargs)
        # df = apply_schema_to_pd(df, schema)
        return cls(df, schema=schema)

    @classmethod
    def from_csv(
        cls, file_path: str, schema: Union[str, List[Dict], TableSchema], sep: str = ",", **kwargs
    ) -> "PandasTable":
        return cls._from_file(file_path, schema, pd.read_csv, sep=sep, **kwargs)

    @classmethod
    def from_big_csv(
        cls, file_path: str, schema: Union[str, List[Dict], TableSchema], sep: str = ",", **kwargs
    ) -> "PandasTable":
        return cls._from_file(file_path, schema, pd.read_csv, sep=sep, engine="pyarrow", dtype_backend="pyarrow")

    @classmethod
    def from_parquet(cls, file_path: str, schema: Union[str, List[Dict], TableSchema], **kwargs) -> "PandasTable":
        return cls._from_file(file_path, schema, pd.read_parquet, **kwargs)

    def missing_info(self, missing_val: Dict[str, Any] = None) -> None:
        missing_info = {}
        total_len = self.shape[0]
        for col in self.columns:
            missing_len = self[col].isna().sum()
            if missing_val is not None:
                missing_len += (self[col] == missing_val.get(col)).sum()
            missing_info[col] = f"{missing_len}/{total_len}: {round(missing_len/total_len * 100, 1)}%"
        print("Missing Info:")
        pprint(missing_info, indent=2, sort_dicts=True)

    @staticmethod
    def plot_pie_bar(s: pd.Series):
        _, ax = plt.subplots(1, 2, figsize=(12, 6))
        value_counts = s.value_counts()
        ax[0].pie(value_counts, autopct="%1.1f%%", shadow=True, explode=[0, 0.1])
        sns.barplot(value_counts, ax=ax[1])
        # 在每个条形上方添加数值标记
        for p in ax[1].patches:
            ax[1].annotate(
                f"{int(p.get_height())}",
                (p.get_x() + p.get_width() / 2.0, p.get_height()),
                ha="center",
                va="center",
                fontsize=10,
                color="black",
                xytext=(0, 5),
                textcoords="offset points",
            )

    @staticmethod
    def plot_prob(s: pd.Series):
        _, ax = plt.subplots(1, 2, figsize=(12, 6))
        sns.histplot(s, ax=ax[0])
        ax[0].set_title(f"Distribution Plot: {s.name}")
        stats.probplot(s, plot=ax[1])

    def distribution(self, target_col: str) -> None:
        nunique_val = self[target_col].nunique()
        if nunique_val < 10 and self[target_col].dtype.name.startswith(("int", "category", "object")):
            self.plot_pie_bar(self[target_col])
        else:
            self.plot_prob(self[target_col])
