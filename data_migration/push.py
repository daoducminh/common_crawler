import polars as pl
import psycopg2
import psycopg2.extras
from config import D_PRODUCT_PARQUET, F_PRICE_PARQUET, TARGET_DATABASE_URL


def push_d_product(conn):
    df = pl.read_parquet(D_PRODUCT_PARQUET)
    rows = df.select(
        ["product_id", "name", "source", "category", "latest_price", "latest_price_at"]
    ).rows()

    sql = """
        insert into d_pc_product (product_id, name, source, category, latest_price, latest_price_at)
        values %s
        on conflict (product_id) do update set
            name = excluded.name,
            source = excluded.source,
            category = excluded.category,
            latest_price = excluded.latest_price,
            latest_price_at = excluded.latest_price_at
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    print(f"pushed d_pc_product: {len(rows)} rows")


def push_f_price(conn):
    df = pl.read_parquet(F_PRICE_PARQUET)
    rows = df.select(["date", "product_id", "price"]).rows()

    sql = """
        insert into f_pc_price (date, product_id, price)
        values %s
        on conflict (product_id, date) do nothing
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    print(f"pushed f_pc_price: {len(rows)} rows")


def refresh_price_daily(conn):
    with conn.cursor() as cur:
        cur.execute("refresh materialized view v_price_daily")
    conn.commit()
    print("refreshed v_price_daily")


def main():
    conn = psycopg2.connect(TARGET_DATABASE_URL)
    try:
        push_d_product(conn)
        push_f_price(conn)
        refresh_price_daily(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
