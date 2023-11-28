from datetime import timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pandas.core.arrays import period
import seaborn as sns


def superstore_to_parq():
    df = pd.read_excel("data/superstore-data.xls")
    df = df.convert_dtypes()

    # basic typing for space efficiency
    df["Quantity"] = df["Quantity"].astype("uint8")
    df["Row ID"] = df["Row ID"].astype("uint16")
    df["Postal Code"] = df["Postal Code"].astype("uint32")
    for x in ["Sales", "Discount", "Profit"]:
        df[x] = df[x].astype("float32")
    for x in [
        "Ship Mode",
        "Customer ID",
        "Customer Name",
        "Segment",
        "Country",
        "City",
        "State",
        "Region",
        "Category",
        "Sub-Category",
        "Order ID",
        "Product ID",
        "Product Name",
    ]:
        df[x] = df[x].astype("category")

    # rename series names for dot notation compliance
    df.rename(str.lower, axis="columns", inplace=True)
    df.columns = df.columns.str.replace("[ -]", "_", regex=True)

    df.to_parquet(
        "data/superstore.snappy.parquet", engine="fastparquet", compression="snappy"
    )


def main():
    # initializes base figure variables
    df = pd.read_parquet("data/superstore.snappy.parquet")
    fig = plt.figure()
    fig.subplots_adjust(wspace=0)
    gs = gridspec.GridSpec(2, 2)

    ax1 = fig.add_subplot(gs[0, 0])
    df_grp_sales = (
        df.groupby("sub_category")
        .sales.sum()
        .reset_index()
        .sort_values("sales", ascending=False)
    )
    temp = df.groupby("category").sub_category.unique().explode().reset_index()
    df_grp_sales = df_grp_sales.merge(temp, on="sub_category")
    sns.barplot(
        data=df_grp_sales,
        y="sub_category",
        x="sales",
        order=df_grp_sales.sub_category,
        ax=ax1,
        hue=df_grp_sales.category,
    )

    ax2 = fig.add_subplot(gs[0, 1])  # , sharey=ax1)
    # ax2.get_yaxis().set_visible(False)
    df_grp_profit = df.groupby("sub_category").profit.sum().reset_index()
    sns.barplot(
        data=df_grp_profit,
        y="sub_category",
        x="profit",
        order=df_grp_sales.sub_category,
        ax=ax2,
        hue=df_grp_sales.category,
    )
    # ax3 = fig.add_subplot(gs[1, 0:])
    plt.show()
    # print(df_grp_sales)


if __name__ == "__main__":
    main()

    # TODO: refactor df_grp_sales and df_grp_profit to one dataframe using previous merge method so graphing is easier, git commit and push
