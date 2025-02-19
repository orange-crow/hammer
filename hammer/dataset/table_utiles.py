from pprint import pprint
from typing import Any, Dict

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from scipy import stats


def missing_info(df: pd.DataFrame, missing_val: Dict[str, Any] = None) -> None:
    missing_info = {}
    total_len = df.shape[0]
    for col in df.columns:
        missing_len = df[col].isna().sum()
        if missing_val is not None:
            missing_len += (df[col] == missing_val.get(col)).sum()
        missing_info[col] = f"{missing_len}/{total_len}: {round(missing_len/total_len * 100, 1)}%"
    print("Missing Info:")
    pprint(missing_info, indent=2, sort_dicts=True)


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
