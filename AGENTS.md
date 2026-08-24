# AGENTS.md

Guide for AI coding agents working in this repository.

## What this repo is

Scrapy project that crawls Vietnamese e-commerce sites (PC hardware prices, gold/gasoline
prices, game items) and stores results in CockroachDB. Deployed to Zyte (formerly
Scrapinghub) via GitHub Actions on push to `main`; scheduled crawls run as GitHub Actions
cron workflows.

## Repo layout

- `common_crawler/settings.py` – base Scrapy settings
- `common_crawler/constants/enums.py` – env-var / setting name constants shared by spiders & pipelines
- `common_crawler/utils/discord.py` – Discord webhook notifier (zero-item warnings)
- `common_crawler/spiders/`
  - `pc/base_spider.py` – HTML/CSS base class for PC-price spiders; subclasses define
    categories + CSS selectors (`appc.py` is the current subclass)
  - `pc/model.py` – SQLAlchemy models for the PC price DB (`pc_price`)
  - `pc/pipeline.py` – `CockroachDBPipeline`, staging-based writer (see mechanic below)
  - `hhpc.py` – JSON AJAX API spider for hoanghapc.vn (Basic-auth token via `-a token=...`),
    uses the same pipeline
  - `cellphones/`, `finances/`, `core_keeper.py` – other targets with their own pipelines/DBs
- `data_migration/` – one-time migration from the legacy single-table schema to dim/fact:
  `pull.py` (paged read of legacy `f_price`) → `transform.py` (polars: build dims + change-only facts)
  → `push.py` (upsert into new tables + refresh view). `sql/` holds the canonical DDL.
- `data_migration/sql/`
  - `new_schema.sql` – `d_pc_product` (dim: product attributes + latest price),
    `f_pc_price` (fact: one row per actual price change per day, `unique(product_id, date)`)
  - `staging.sql` – `stg_pc_price`: crawl buffer, PK `(source, product_id)`
  - `views.sql` – `v_price_daily` materialized view: forward-filled price-per-product-per-day
    for Looker Studio, must be refreshed after data changes
  - `old_schema.sql` – legacy `f_price` big table (reference only)

## PC price storing mechanic

Spiders yield `{source, product_id, name, category, price, crawled_at}` where
`product_id = "{source}__{native_id}"`. This format **must stay aligned** with
`data_migration/transform.py` so migrated history joins live data.

Pipeline flow per crawl run:

1. `open_spider` (begin): clear `stg_pc_price`.
2. `process_item`: upsert each crawled item into `stg_pc_price`
   (`on conflict (source, product_id)`) — chunk retries are idempotent.
3. `close_spider` (finish), one transaction:
   fact rows for prices differing from `d_pc_product.latest_price` → upsert new/changed
   dim rows (incl. rename/recategorisation) → first fact row for brand-new products →
   clear staging. Afterwards: `refresh materialized view v_price_daily`.

Consequences: unchanged prices never create fact rows; the dim always stores the newest
observed price so the next run can diff against it.

## Environment variables

Pipeline reads scrapy settings named after these constants, falling back to OS env when
`ENV=dev` (local runs, GitHub Actions):

- `ENV` – set to `dev` outside Zyte
- `DATABASE_URL` – full CockroachDB Cloud connection string, e.g.
  `cockroachdb://USER:PASSWORD@HOST:26257/pc_price?sslmode=verify-full`
  (`sslrootcert=/path/ca.crt` optional; sslmode params pass through to psycopg2)
- `WARNING_DISCORD_WEBHOOK` – zero-item alerts
- HHPC auth token is a spider argument: `-a token=...` (GitHub secret `HHPC_TOKEN`)

## Commands

```bash
uv sync                                        # install deps (Python 3.12)
uv run scrapy list                             # list spiders
ENV=dev DATABASE_URL=cockroachdb://USER:PASSWORD@HOST:26257/pc_price?sslmode=verify-full \
  uv run scrapy crawl hhpc -a token=$HHPC_TOKEN   # run locally / in CI
uv export --format requirements-txt > requirements.txt  # used by Zyte deploy
```

## Conventions

- Keep `common_crawler/spiders/pc/model.py` in sync with `data_migration/sql/*.sql`;
  the SQL files are the source of truth.
- Never write to `f_pc_price` / `d_pc_product` directly from spiders — always through the
  staging pipeline.
- Timezone is Asia/Ho_Chi_Minh (`TZ_HCM`); use pendulum.
- SQLAlchemy 2.0 style (`DeclarativeBase`, `Mapped`, `mapped_column`).
- New PC-price spider = subclass `PCBaseSpider`, set `name`, `base_url`, `categories` and
  the five CSS selector attributes; item shape comes for free.
