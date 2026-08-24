import polars as pl
import psycopg2
from config import CHUNK_SIZE, RAW_DIR, SOURCE_DATABASE_URL, SOURCE_TABLE


def pull():
    conn = psycopg2.connect(SOURCE_DATABASE_URL)
    last_rowid = -1
    part = 0
    while True:
        query = f"""
            SELECT id, timestamp, source, name, price, category, rowid, ingest_date
            FROM {SOURCE_TABLE}
            WHERE rowid > {last_rowid}
            ORDER BY rowid
            LIMIT {CHUNK_SIZE}
        """
        df = pl.read_database(query=query, connection=conn)
        if df.is_empty():
            break

        out_path = RAW_DIR / f"f_price_{part:05d}.parquet"
        df.write_parquet(out_path)
        print(f"wrote {out_path} rows={df.height}")

        last_rowid = df["rowid"][-1]
        part += 1
        if df.height < CHUNK_SIZE:
            break

    conn.close()


if __name__ == "__main__":
    pull()
