from datetime import timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas.core.arrays import period
import seaborn as sns


def superstore_to_parq():
    df = pd.read_excel("data/superstore-data.xls")
    df = df.convert_dtypes()

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
    df.to_parquet(
        "data/superstore.snappy.parquet", engine="fastparquet", compression="snappy"
    )


def main():
    df = pd.read_parquet("data/superstore.snappy.parquet")
    dates = df["Order Date"].values.astype("datetime64[M]")
    datecount = np.unique(dates, return_counts=True)
    sns.histplot(
        x=datecount[0],
        y=datecount[1],
        # binrange=pd.date_range(dates.min(), end=dates.max(), freq="6M").values.astype(
        #     "datetime64[M]"
        # ),
    )
    print(
        type(dates.min()),
        pd.date_range(dates.min(), end=dates.max(), freq="6M").values.astype(
            "datetime64[M]"
        ),
    )


if __name__ == "__main__":
    main()
