create table if not exists d_pc_product (
  product_id text primary key,
  name text,
  source text,
  category text,
  latest_price bigint,
  latest_price_at timestamptz
);

create table if not exists f_pc_price (
  id bigserial primary key,
  date date not null,
  product_id text not null references d_pc_product(product_id),
  price bigint not null,
  unique (product_id, date)
);

create index if not exists f_pc_price_product_id_idx on f_pc_price(product_id);
create index if not exists f_pc_price_date_idx on f_pc_price(date);
