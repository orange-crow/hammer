import fire
import seaborn as sns
from matplotlib import pyplot as plt

from hammer.dataset import PandasTable

user_info_file = "examples/audience_expansion/data/data_format1/user_info_format1.csv"
train_file = "examples/audience_expansion/data/data_format1/train_format1.csv"
test_file = "examples/audience_expansion/data/data_format1/test_format1.csv"


def user_age():
    df = PandasTable.from_csv(user_info_file, "user_id: str, entity; age_range: str; gender:str")
    # 全量数据年龄分布
    age_data = df.groupby("age_range")["user_id"].count().reset_index()
    age_data.columns = ["age_range", "cnt_user"]
    # 训练集和测试集上年龄，性别分布
    train_data = PandasTable.from_csv(train_file, "user_id: str;merchant_id:str;label:int")
    test_data = PandasTable.from_csv(test_file, "user_id: str;merchant_id:str;prob:str")
    train_data = train_data.merge(df, on="user_id", how="left")
    test_data = test_data.merge(df, on="user_id", how="left")

    train_age = train_data.groupby("age_range")["user_id"].count().reset_index()[["age_range", "user_id"]]
    train_age.columns = ["age_range", "cnt_user"]

    test_age = test_data.groupby("age_range")["user_id"].count().reset_index()[["age_range", "user_id"]]
    test_age.columns = ["age_range", "cnt_user"]

    #  年龄和label的散点图
    train_age_label = train_data.groupby(["age_range", "label"])["user_id"].count().reset_index()
    train_age_label.columns = ["age_range", "label", "cnt_user"]

    #  绘图
    fig, ax = plt.subplots(1, 4, figsize=(12, 3))
    age_data.plot(x="age_range", y="cnt_user", kind="bar", ax=ax[0], title="Total")
    train_age.plot(x="age_range", y="cnt_user", kind="bar", ax=ax[1], title="Train")
    test_age.plot(x="age_range", y="cnt_user", kind="bar", ax=ax[2], title="Test")
    ax[3] = sns.barplot(data=train_age_label, x="age_range", y="cnt_user", hue="label")
    ax[3].set_yscale("log")
    plt.show()


def eda_missing():
    df = PandasTable.from_csv(user_info_file, "user_id: str, entity; age_range: str; gender:str")
    df.missing_info({"age_range": 0})


def eda_distribution():
    df = PandasTable.from_big_csv(train_file, "user_id: str, entity; merchant_id: str; label: int")
    df.distribution("label")


if __name__ == "__main__":
    fire.Fire()
