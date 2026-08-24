import logging

from scrapy import Spider
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from common_crawler.utils.db import build_engine

from .model import StgPcPrice

logger = logging.getLogger(__name__)


class CockroachDBPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings.copy_to_dict()
        return cls(settings)

    def __init__(self, settings: dict) -> None:
        self.engine = build_engine(settings)
        self.session = Session(self.engine)

    def open_spider(self):
        self.session.execute(text("delete from stg_pc_price"))
        self.session.commit()

    def close_spider(self):
        try:
            self._finish()
        except Exception:
            self.session.rollback()
            logger.exception(
                "finish transaction failed, stg_pc_price kept for debugging"
            )
            raise
        finally:
            self.session.close()
            self.engine.dispose()

    def process_item(self, item, spider: Spider):
        try:
            data = {
                "source": item["source"],
                "product_id": item["product_id"],
                "name": item["name"],
                "category": item["category"],
                "price": int(item["price"]),
                "crawled_at": item["crawled_at"],
            }
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("Skipping invalid item %r: %s", item.get("product_id"), e)
            return item

        stmt = insert(StgPcPrice).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source", "product_id"],
            set_={
                "name": stmt.excluded.name,
                "category": stmt.excluded.category,
                "price": stmt.excluded.price,
                "crawled_at": stmt.excluded.crawled_at,
            },
        )
        self.session.execute(stmt)
        self.session.commit()
        return item

    _FINISH_STATEMENTS = [
        # fact rows for existing products whose price changed (dim still holds
        # the old latest_price at this point)
        """
        insert into f_pc_price (date, product_id, price)
        select s.crawled_at, s.product_id, s.price::bigint
        from stg_pc_price s
        join d_pc_product d on d.product_id = s.product_id
        where d.latest_price is distinct from s.price
        on conflict (product_id, date) do nothing
        """,
        # brand-new products -> dim
        """
        insert into d_pc_product (product_id, name, source, category, latest_price, latest_price_at)
        select s.product_id, s.name, s.source, s.category, s.price::bigint, s.crawled_at::timestamptz
        from stg_pc_price s
        left join d_pc_product d on d.product_id = s.product_id
        where d.product_id is null
        on conflict (product_id) do nothing
        """,
        # first-ever fact row for brand-new products
        """
        insert into f_pc_price (date, product_id, price)
        select s.crawled_at, s.product_id, s.price::bigint
        from stg_pc_price s
        where not exists (
            select 1 from f_pc_price f where f.product_id = s.product_id
        )
        on conflict (product_id, date) do nothing
        """,
        # dim latest price refresh for changed products
        """
        update d_pc_product d
        set latest_price = s.price::bigint,
            latest_price_at = s.crawled_at::timestamptz,
            name = s.name,
            category = s.category
        from stg_pc_price s
        where s.product_id = d.product_id
          and d.latest_price is distinct from s.price
        """,
        # metadata drift (rename / recategorisation) without price change
        """
        update d_pc_product d
        set name = s.name,
            category = s.category
        from stg_pc_price s
        where s.product_id = d.product_id
          and (d.name is distinct from s.name or d.category is distinct from s.category)
        """,
        # done -> empty staging inside the finish transaction
        "delete from stg_pc_price",
    ]

    def _finish(self):
        staged = self.session.execute(text("select count(*) from stg_pc_price")).scalar()
        logger.info("Finishing run with %s staged items", staged)

        for stmt in self._FINISH_STATEMENTS:
            result = self.session.execute(text(stmt))
            if result.rowcount and result.rowcount > 0:
                logger.info("%s rows affected by: %s", result.rowcount, stmt.strip()[:80])

        self.session.commit()

        try:
            self.session.execute(text("refresh materialized view v_price_daily"))
            self.session.commit()
        except Exception:
            self.session.rollback()
            logger.exception("failed to refresh v_price_daily")
