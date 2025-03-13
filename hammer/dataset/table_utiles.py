from typing import Any, Dict

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from scipy import stats


def missing_info(df: pd.DataFrame, missing_val: Dict[str, Any] = None) -> None:
    # missing_info = {}
    # total_len = df.shape[0]
    # for col in df.columns:
    #     missing_len = df[col].isna().sum()
    #     if missing_val is not None:
    #         missing_len += (df[col] == missing_val.get(col)).sum()
    #     missing_info[col] = f"{missing_len}/{total_len}: {round(missing_len/total_len * 100, 1)}%"
    # print("Missing Info:")
    # pprint(missing_info, indent=2, sort_dicts=True)
    if missing_val is None:
        return df.isnull().sum()
    is_null = False
    for col_name, null_val in missing_val.items():
        is_null |= df[col_name] == null_val

    for col_name in missing_val.keys():
        is_null |= df[col_name].isnull()

    print(df[is_null].count())


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


def plot_prob(s: pd.Series):
    _, ax = plt.subplots(1, 2, figsize=(12, 6))
    sns.histplot(s, ax=ax[0])
    ax[0].set_title(f"Distribution Plot: {s.name}")
    stats.probplot(s, plot=ax[1])


def reduce_memory(df: pd.DataFrame):
    # 打印压缩前的内存占用
    print(f"Memory usage before compression: {df.memory_usage(deep=True).sum() / 1024**2} MB")

    # 压缩数据类型
    for col in df.select_dtypes(include=["int64", "float64"]).columns:
        min_value = df[col].min()
        max_value = df[col].max()

        if df[col].dtype == "int64":
            # 根据数据范围压缩int类型
            if min_value >= 0:
                if max_value < 2**8:
                    df[col] = df[col].astype("uint8")
                elif max_value < 2**16:
                    df[col] = df[col].astype("uint16")
                elif max_value < 2**32:
                    df[col] = df[col].astype("uint32")
                else:
                    df[col] = df[col].astype("uint64")
            else:
                if min_value >= -(2**7) and max_value < 2**7:
                    df[col] = df[col].astype("int8")
                elif min_value >= -(2**15) and max_value < 2**15:
                    df[col] = df[col].astype("int16")
                elif min_value >= -(2**31) and max_value < 2**31:
                    df[col] = df[col].astype("int32")
                else:
                    df[col] = df[col].astype("int64")
        elif df[col].dtype == "float64":
            # 压缩float类型
            df[col] = df[col].astype("float32")

    # 打印压缩后的内存占用
    print(f"Memory usage after compression: {df.memory_usage(deep=True).sum() / 1024**2} MB")

    return df
