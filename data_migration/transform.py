import polars as pl
from config import D_PRODUCT_PARQUET, F_PRICE_PARQUET, RAW_PARQUET_GLOB


def transform():
    df = (
        pl.scan_parquet(RAW_PARQUET_GLOB)
        .select(["id", "timestamp", "source", "name", "price", "category"])
        .filter(
            pl.col("id").is_not_null()
            & pl.col("source").is_not_null()
            & pl.col("price").is_not_null()
            & pl.col("timestamp").is_not_null()
        )
        .with_columns((pl.col("source") + "__" + pl.col("id")).alias("product_id"))
        .sort(["product_id", "timestamp"])
        .collect()
    )

    d_product = df.group_by("product_id", maintain_order=True).agg(
        [
            pl.col("name").last().alias("name"),
            pl.col("source").last().alias("source"),
            pl.col("category").last().alias("category"),
            pl.col("price").last().alias("latest_price"),
            pl.col("timestamp").last().alias("latest_price_at"),
        ]
    )

    f_price = (
        df.with_columns(pl.col("price").shift(1).over("product_id").alias("prev_price"))
        .filter(
            (pl.col("prev_price").is_null()) | (pl.col("price") != pl.col("prev_price"))
        )
        .select(
            [
                pl.col("timestamp").dt.date().cast(pl.Utf8).alias("date"),
                pl.col("product_id"),
                pl.col("price"),
            ]
        )
    )

    d_product.write_parquet(D_PRODUCT_PARQUET)
    f_price.write_parquet(F_PRICE_PARQUET)

    print(f"d_product: {d_product.height} rows -> {D_PRODUCT_PARQUET}")
    print(f"f_price: {f_price.height} rows -> {F_PRICE_PARQUET}")


if __name__ == "__main__":
    transform()
