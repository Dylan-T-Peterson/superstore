import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
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
    def numfmt(x, pos):
        s = "{}".format(int(x / 1000))
        return s

    xfmt = mpl.ticker.FuncFormatter(numfmt)

    # initializes base figure variables
    df = pd.read_parquet("data/superstore.snappy.parquet")
    fig = plt.figure()
    fig.subplots_adjust(wspace=0)
    gs = gridspec.GridSpec(2, 2)
    zordlist = np.arange(10, step=0.5)

    # initializes main graphing dataframe
    # temp_df used to add correct category information
    # before adding to main grouped dataframe
    df_grp = df.groupby("sub_category")[["sales", "profit"]].sum().reset_index()
    temp_df = df.groupby("category").sub_category.unique().explode().reset_index()
    df_grp = df_grp.merge(temp_df, on="sub_category")
    df_grp["margins"] = (df_grp.profit / df_grp.sales) * 100
    df_grp = df_grp[["sub_category", "category", "sales", "profit", "margins"]]
    df_grp.sort_values("sales", ascending=False, inplace=True)

    # creation of chart 1 (upper left)
    ax1 = fig.add_subplot(gs[0, 0])
    plt.setp(ax1, ylabel="Sub Category", xlabel="Sales (In Thousands)")
    sns.barplot(
        data=df_grp,
        y="sub_category",
        x="sales",
        order=df_grp.sub_category,
        ax=ax1,
        hue=df_grp.category,
        zorder=zordlist[-1],
    )
    for num in np.arange(stop=325_001, step=25_000):
        ax1.axvline(num, zorder=zordlist[0], alpha=0.25)
    ax1.xaxis.set_major_formatter(xfmt)

    # creation of chart 2 (upper right)
    ax2 = fig.add_subplot(gs[0, 1], sharey=ax1)
    sns.barplot(
        data=df_grp,
        y="sub_category",
        x="profit",
        order=df_grp.sub_category,
        ax=ax2,
        hue=df_grp.category,
        zorder=zordlist[-2],
    )
    for num in np.arange(start=-20_000, stop=55_001, step=5_000):
        ax2.axvline(num, zorder=zordlist[0], alpha=0.25)
    ax2.axvline(0, zorder=zordlist[-1], color="k")
    ax2.yaxis.tick_right()
    plt.setp(ax2, ylabel=" ", xlabel="Profit (In Thousands)")
    ax2.xaxis.set_major_formatter(xfmt)

    # creation of chart 3 (bottom)
    ax3 = fig.add_subplot(gs[1, 0:])
    sns.barplot(
        data=df_grp,
        x="sub_category",
        y="margins",
        order=df_grp.sub_category,
        ax=ax3,
        hue=df_grp.category,
        zorder=zordlist[-2],
    )
    for num in np.arange(start=-10, stop=46, step=5):
        ax3.axhline(num, zorder=zordlist[0], alpha=0.25)
    ax3.axhline(0, zorder=zordlist[-1], color="k")
    plt.setp(ax3.get_xticklabels(), rotation=45, ha="right")
    plt.setp(ax3, xlabel="Sub Category", ylabel="Profit Margins")
    ax3.yaxis.set_major_formatter(lambda x, pos: f"{int(x)}%")

    # fig.set()

    # plt.setp(ax1)
    plt.show()
    # print(ax1.getx)


if __name__ == "__main__":
    main()
