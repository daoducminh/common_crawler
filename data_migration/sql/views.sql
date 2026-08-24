-- Forward-fills f_pc_price (change-only rows) into one row per product per day,
-- joined with d_pc_product attributes so Looker Studio can filter on
-- date, name, source, category directly from this single view.

create materialized view if not exists v_price_daily as
with product_range as (
    select product_id, min(date) as start_date, current_date as end_date
    from f_pc_price
    group by product_id
),
date_grid as (
    select p.product_id, gs::date as date
    from product_range p,
         generate_series(p.start_date::timestamp, p.end_date::timestamp, interval '1 day') as gs
),
joined as (
    select
        g.product_id,
        g.date,
        fp.price,
        count(fp.price) over (partition by g.product_id order by g.date) as grp
    from date_grid g
    left join f_pc_price fp
        on fp.product_id = g.product_id and fp.date = g.date
)
select
    j.date,
    j.product_id,
    d.name,
    d.source,
    d.category,
    first_value(j.price) over (
        partition by j.product_id, j.grp order by j.date
    ) as price
from joined j
join d_pc_product d on d.product_id = j.product_id;

create unique index if not exists v_price_daily_pk
    on v_price_daily (product_id, date);

create index if not exists v_price_daily_date_idx on v_price_daily (date);
create index if not exists v_price_daily_name_idx on v_price_daily (name);
create index if not exists v_price_daily_source_idx on v_price_daily (source);
create index if not exists v_price_daily_category_idx on v_price_daily (category);
