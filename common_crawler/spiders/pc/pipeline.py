import logging
import os

from scrapy import Spider
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .model import ItemPrice

logger = logging.getLogger(__name__)


class CockroachDBPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings.copy_to_dict()
        return cls(settings)

    def __init__(self, settings: dict) -> None:
        db_host = settings.get("DB_HOST")
        db_port = settings.get("DB_PORT")
        db_user = settings.get("DB_USER")
        db_password = settings.get("DB_PASSWORD")
        db_name = "pc_price"

        env = os.getenv("ENV")
        if env == "dev":
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT")
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")

        self.engine = create_engine(
            f"cockroachdb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )
        self.session = Session(self.engine)

    def open_spider(self):
        pass

    def close_spider(self):
        # self.session.commit()
        self.session.close()
        self.engine.dispose()

    def process_item(self, item, spider: Spider):
        data = ItemPrice(**item)

        self.session.add(data)
        self.session.commit()
        return item
