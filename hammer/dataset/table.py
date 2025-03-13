from pprint import pprint
from typing import Any, Callable, Dict, List, Sequence, Union

import numpy as np
import pandas as pd
from pandas._libs import lib
from pandas._typing import Axis, CorrelationMethod, IndexLabel, TakeIndexer
from pandas.core.groupby import DataFrameGroupBy
from pandas.core.groupby.groupby import GroupByPlot
from pandas.core.indexing import _LocIndexer

from ..schema import TableSchema
from ..utils.schema import init_schema
from .table_base import TableBase
from .table_utiles import (
    interaction_bar,
    missing_info,
    plot_pie_bar,
    plot_prob,
    reduce_memory,
)


def wrape_result(result: Any):
    if isinstance(result, pd.DataFrame):
        result = PandasTable(result)
    elif isinstance(result, pd.Series):
        result = PandasSeries(result)
    return result


class PandasTableGroupBy(object):
    """
    这是一个包装类，用于将 groupby 的结果转换为 PandasTableGroupBy。
    它不是直接继承 DataFrameGroupBy, 而是将 groupby 的结果封装在内部。
    """

    def __init__(self, groupby_result: DataFrameGroupBy, by):
        self.groupby_result: DataFrameGroupBy = groupby_result
        self.by = by

    def __getitem__(self, key):
        """
        允许通过列名选择特定的列，例如 df.groupby("merchant_id")["label"]。
        """
        if isinstance(key, str):
            # 通过列名选择对应的列
            return PandasTableGroupBy(self.groupby_result[key], self.by)
        else:
            raise KeyError(f"Key '{key}' is not valid for PandasTableGroupBy.")

    def mean(self, **kwargs) -> "PandasSeries":
        """
        计算每个组的均值，并返回 PandasTable 类型的结果。
        """
        result = self.groupby_result.mean(**kwargs)
        return wrape_result(result)

    def sum(self, **kwargs) -> "PandasSeries":
        result = self.groupby_result.sum(**kwargs)
        return wrape_result(result)

    def apply(self, func, **kwargs) -> Union["PandasSeries", "PandasTable"]:
        """
        应用自定义函数并返回 PandasTable 类型的结果。
        """
        result = self.groupby_result.apply(func, **kwargs)
        return wrape_result(result)

    def transform(self, func, *args, engine=None, engine_kwargs=None, **kwargs) -> Union["PandasSeries", "PandasTable"]:
        result = self.groupby_result.transform(func, *args, engine, engine_kwargs, **kwargs)
        return wrape_result(result)

    def filter(self, func, dropna: bool = True, *args, **kwargs) -> Union["PandasSeries", "PandasTable"]:
        result = self.groupby_result.filter(func, dropna, *args, **kwargs)
        return wrape_result(result)

    def nunique(self, dropna: bool = True) -> Union["PandasSeries", "PandasTable"]:
        result = self.groupby_result.nunique(dropna)
        return wrape_result(result)

    def value_counts(
        self,
        subset=None,
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        dropna: bool = True,
    ) -> Union["PandasSeries", "PandasTable"]:
        result = self.groupby_result.value_counts(subset, normalize, sort, ascending, dropna)
        return wrape_result(result)

    def fillna(
        self,
        value: pd.Series | pd.DataFrame | None = None,
        method: None = None,
        axis: None | lib.NoDefault = lib.no_default,
        inplace: bool = False,
        limit: int | None = None,
        downcast=lib.no_default,
    ) -> Union["PandasTable", None]:
        result = self.groupby_result.fillna(value, method, axis, inplace, limit, downcast)
        return wrape_result(result)

    def take(
        self,
        indices: TakeIndexer,
        axis: Axis | None | lib.NoDefault = lib.no_default,
        **kwargs,
    ) -> "PandasTable":
        result = self.groupby_result.take(indices, axis, **kwargs)
        return wrape_result(result)

    def skew(
        self,
        axis: Axis | None | lib.NoDefault = lib.no_default,
        skipna: bool = True,
        numeric_only: bool = False,
        **kwargs,
    ) -> "PandasTable":
        result = self.groupby_result.skew(axis, skipna, numeric_only, **kwargs)
        return wrape_result(result)

    @property
    def plot(self) -> "GroupByPlot":
        result = self.groupby_result.plot
        return result

    def corr(
        self,
        method: str | Callable[[np.ndarray, np.ndarray], float] = "pearson",
        min_periods: int = 1,
        numeric_only: bool = False,
    ) -> "PandasTable":
        result = self.groupby_result.corr(method, min_periods, numeric_only)
        return wrape_result(result)

    def cov(
        self,
        min_periods: int | None = None,
        ddof: int | None = 1,
        numeric_only: bool = False,
    ) -> "PandasTable":
        result = self.groupby_result.cov(min_periods, ddof, numeric_only)
        return wrape_result(result)

    def hist(
        self,
        column: IndexLabel | None = None,
        by=None,
        grid: bool = True,
        xlabelsize: int | None = None,
        xrot: float | None = None,
        ylabelsize: int | None = None,
        yrot: float | None = None,
        ax=None,
        sharex: bool = False,
        sharey: bool = False,
        figsize: tuple[int, int] | None = None,
        layout: tuple[int, int] | None = None,
        bins: int | Sequence[int] = 10,
        backend: str | None = None,
        legend: bool = False,
        **kwargs,
    ):
        self.groupby_result.hist(
            column,
            by,
            grid,
            xlabelsize,
            xrot,
            ylabelsize,
            yrot,
            ax,
            sharex,
            sharey,
            figsize,
            layout,
            bins,
            backend,
            legend,
            **kwargs,
        )

    @property
    def dtypes(self) -> pd.Series:
        return self.groupby_result.dtypes

    def corrwith(
        self,
        other: pd.DataFrame | pd.Series | Union["PandasSeries", "PandasTable"],
        axis: Axis | lib.NoDefault = lib.no_default,
        drop: bool = False,
        method: CorrelationMethod = "pearson",
        numeric_only: bool = False,
    ) -> "PandasTable":
        result = self.groupby_result.corrwith(other, axis, drop, method, numeric_only)
        return wrape_result(result)


class PandasTableLocIndexer(_LocIndexer):
    """
    自定义 _LocIndexer, 确保 loc 操作返回 PandasTable 类型。
    """

    def __init__(self, name: str, df):
        self._df = df
        super().__init__(name, df)

    def __getitem__(self, key) -> Union["PandasSeries", "PandasTable"]:
        """
        重写索引操作，确保结果是 PandasTable 类型。
        """
        result = super(PandasTableLocIndexer, self).__getitem__(key)
        return wrape_result(result)


class PandasSeries(pd.Series):
    def __finalize__(self, other, method=None, **kwargs):
        """
        重写 __finalize__ 方法，以确保链式操作时能够保持自定义属性。
        """
        result = super(PandasSeries, self).__finalize__(other, method=method, **kwargs)
        return result

    def custom_method(self):
        """
        一个自定义方法，用于演示如何扩展 pd.Series 的功能。
        """
        return self.sum()  # 返回所有元素的和，您可以根据需要修改该方法

    def __getitem__(self, key):
        """
        重写 __getitem__ 方法，确保自定义属性也能随之传递。
        """
        result = super(PandasSeries, self).__getitem__(key)
        if isinstance(result, pd.Series):
            result = PandasSeries(result)
        return result

    @property
    def loc(self):
        return PandasTableLocIndexer("loc", self)

    def missing_info(self, missing_val: Any = None) -> None:
        missing_len = self.isna().sum()
        if missing_val is not None:
            missing_len += (self == missing_val).sum()
        pprint(f"{self.name}: {missing_len}/{len(self)}, {round(missing_len/len(self) * 100, 1)}%")

    def plot_pie_bar(self):
        plot_pie_bar(self)

    def plot_prob(self):
        plot_prob(self)

    def distribution(self) -> None:
        nunique_val = self.nunique()
        if nunique_val < 10 and self.dtype.name.startswith(("int", "category", "object")):
            self.plot_pie_bar()
        else:
            self.plot_prob()


class PandasTable(pd.DataFrame, TableBase):
    _metadata = ["_schema"]  # 保留自定义属性

    def __init__(self, *args, **kwargs):
        schema = kwargs.pop("schema", None)  # 从 kwargs 中取出 schema
        super(PandasTable, self).__init__(*args, **kwargs)
        if schema is not None:
            self._schema = init_schema(schema)
        else:
            self._schema = None

    def __finalize__(self, other, method=None, **kwargs) -> Union["PandasTable", Any]:
        """
        覆盖 __finalize__ 以确保链式操作返回 PandasTable, 而不是 pd.DataFrame。
        """
        # 确保从 `other`（可能是一个 pd.DataFrame）中继承元数据
        # 但返回的对象仍然是 PandasTable 类型
        result = super(PandasTable, self).__finalize__(other, method=method, **kwargs)

        # 保留自定义的 schema 属性
        if hasattr(other, "_schema"):
            result._schema = other._schema

        return wrape_result(result)

    def __str__(self):
        return super().__str__() + f"\n{self.table_schema}"

    def __getitem__(self, key):
        """
        重写 __getitem__ 以确保通过索引操作返回 PandasTable 类型。
        """
        result = super(PandasTable, self).__getitem__(key)
        return wrape_result(result)

    def __setitem__(self, key, value):
        """
        重写 __setitem__ 以确保在设置值时返回 PandasTable 类型。
        """
        super(PandasTable, self).__setitem__(key, value)
        return self

    @property
    def table_schema(self) -> TableSchema:
        if self._schema is None:
            self._schema = init_schema([{k: v} for k, v in self.dtypes.to_dict().items()])
        return self._schema

    def update_schema(self) -> None:
        raise NotImplementedError

    def set_table_schema(self, table_schema: Union[str, List[Dict]]) -> None:
        if isinstance(table_schema, str):
            self._schema = TableSchema.from_string(table_schema)
        elif isinstance(table_schema, List) and isinstance(table_schema[0], Dict):
            self._schema = TableSchema.from_list(table_schema)
        raise ValueError(f"Error value for table_schema: {table_schema}, only support str or list[dict]")

    @property
    def loc(self):
        """
        覆盖 loc 函数，确保 loc 操作返回 PandasTable 类型。
        """
        return PandasTableLocIndexer("loc", self)

    def groupby(self, by, *args, **kwargs):
        """
        重写 groupby 方法，确保返回 PandasTable。
        """
        # 返回的是 PandasTable 而不是 GroupBy 对象
        groupby_result = super(PandasTable, self).groupby(by, *args, **kwargs)

        # 由于 groupby 返回的是 GroupBy 对象，我们通过 apply 强制转换回 PandasTable
        return PandasTableGroupBy(groupby_result, by)

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
        missing_info(self, missing_val)

    @staticmethod
    def plot_pie_bar(s: pd.Series):
        plot_pie_bar(s)

    @staticmethod
    def plot_prob(s: pd.Series):
        plot_prob(s)

    def distribution(self, target_col: str, x: str = None) -> None:
        nunique_val = self[target_col].nunique()
        if nunique_val < 10 and self[target_col].dtype.name.startswith(("int", "category", "object")):
            if x is None:
                self.plot_pie_bar(self[target_col])
            else:
                interaction_bar(x, target_col, self)
        else:
            self.plot_prob(self[target_col])

    def reduce_memory(self) -> "PandasTable":
        reduce_memory(self)

    def copy(self, deep=...):
        df = super().copy(deep)
        return PandasTable(df, schema=self._schema)
