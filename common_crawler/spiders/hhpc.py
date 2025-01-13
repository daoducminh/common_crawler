# -*- coding: utf-8 -*-

import pendulum
from scrapy import Request, Spider
from scrapy.http.response import Response
from sqlalchemy import create_engine, String, Integer, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

SOURCE = "hhpc"
BASE_URL = "https://hoanghapc.vn"
PC_CATEGORIES = [
    "pc-workstation",
    "hhpc-workstation-render-edit-video",
    "pc-dep",
    "pc-gaming",
]


class Base(DeclarativeBase):
    pass


class ItemPrice(Base):
    __tablename__ = "f_price"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    price: Mapped[int] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(String)
    category: Mapped[str] = mapped_column(String)
    timestamp: Mapped[pendulum.datetime] = mapped_column(TIMESTAMP("Asia/Ho_Chi_Minh"))


class CockroachDBPipeline:
    def __init__(self) -> None:
        pass

    def open_spider(self, spider: Spider):
        settings = spider.settings.copy_to_dict()
        db_host = settings.get("DB_HOST")
        db_port = settings.get("DB_PORT")
        db_user = settings.get("DB_USER")
        db_password = settings.get("DB_PASSWORD")
        db_name = settings.get("DB_NAME")

        self.engine = create_engine(
            f"cockroachdb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )
        self.session = Session(self.engine)

    def close_spider(self, spider):
        self.session.close()

    def process_item(self, item, spider: Spider):
        self.session.add(ItemPrice(**item))
        self.session.commit()
        return item


class HHPC(Spider):
    name = SOURCE

    custom_settings = {
        "LOG_LEVEL": "INFO",
        "ITEM_PIPELINES": {
            "common_crawler.spiders.hhpc.CockroachDBPipeline": 300,
        },
    }

    def start_requests(self):
        for category in PC_CATEGORIES:
            yield Request(f"{BASE_URL}/{category}", self.parse_product_page)

    def parse_product_page(self, response: Response):
        for page in response.css(".paging a::attr(href)").getall():
            next_url = response.urljoin(page)
            yield Request(next_url, self.parse_product_page)

        for p in response.css(".p-container .p-item"):
            try:
                price_str = p.css(".p-price::text").get()
                price = int(price_str.replace("đ", "").replace(".", "").strip())

                yield {
                    "id": p.css(".p-name::attr(href)").get().split("/")[-1],
                    "name": p.css(".p-name h3::text").get().strip(),
                    "price": price,
                    "source": SOURCE,
                    "category": response.css(".page-title::text").get().strip(),
                    "timestamp": pendulum.now(tz="Asia/Ho_Chi_Minh"),
                }
            except Exception as e:
                self.logger.error(e)
