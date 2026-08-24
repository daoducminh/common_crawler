import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "data" / "output"
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_DATABASE_URL = os.environ["SOURCE_DATABASE_URL"]
TARGET_DATABASE_URL = os.environ["TARGET_DATABASE_URL"]

SOURCE_TABLE = "public.f_price"
CHUNK_SIZE = int(os.environ.get("PULL_CHUNK_SIZE", "200000"))

RAW_PARQUET_GLOB = str(RAW_DIR / "f_price_*.parquet")
D_PRODUCT_PARQUET = OUTPUT_DIR / "d_product.parquet"
F_PRICE_PARQUET = OUTPUT_DIR / "f_price.parquet"
