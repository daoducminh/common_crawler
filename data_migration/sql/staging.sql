-- Staging table for the GAS -> Cloudflare Worker -> CockroachDB PC price
-- pipeline. The crawler stages EVERYTHING it crawls here (PUT), then a single
-- finish action computes the real price changes against f_pc_price history,
-- updates d_pc_product, refreshes v_price_daily, and clears this table.
--
-- It is a NORMAL table (not TEMPORARY): Worker/GAS requests span many
-- connections, so the data must persist between them. Lifecycle:
--   - cleared at the start of every run (POST {action:"begin"})
--   - cleared again at the end of a successful run (inside the finish
--     transaction, so a half-finished run leaves data for debugging until the
--     next begin clears it)
--
-- Primary key (source, product_id) makes chunk retries idempotent: a retried
-- PUT simply overwrites the same row.
CREATE TABLE IF NOT EXISTS stg_pc_price (
    source     STRING  NOT NULL,
    product_id STRING  NOT NULL,
    name       STRING  NOT NULL,
    category   STRING  NOT NULL,
    price      DECIMAL NOT NULL,
    crawled_at DATE    NOT NULL,
    PRIMARY KEY (source, product_id)
);

-- The Worker empties the table with DELETE FROM stg_pc_price (not TRUNCATE)
-- because the end-of-run clear runs inside the finish transaction, where a
-- row-level delete is safely rollback-able. Manual cleanup can use either.
