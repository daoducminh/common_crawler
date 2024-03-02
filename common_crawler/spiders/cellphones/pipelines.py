import os
import re
import logging
import pendulum
from scrapy import Spider
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from w3lib.html import remove_tags
from common_crawler.spiders.cellphones.constants import DEFAULT_TZ, PHONE_CATE_ID
from common_crawler.spiders.cellphones.models import ItemPrice

logger = logging.getLogger(__name__)


def clean_basic_info(text: str | None) -> str | None:
    if not text:
        return text

    text = remove_tags(text)

    text = re.sub(r"\s+", " ", text).strip()
    return text


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
        # transform item
        ingest_time = item.get("ingest_time", pendulum.now(tz=DEFAULT_TZ))
        ingest_date: pendulum.Date = ingest_time.date()
        ingest_date_str = ingest_date.isoformat()

        has_nfc = False
        nfc = item.get("nfc")
        if nfc and nfc == "Có":
            has_nfc = True

        category_id = item.get("category_id", PHONE_CATE_ID)
        product_id = item["id"]

        data = {
            "id": f"{ingest_date_str}_{category_id}_{product_id}",
            "name": item["name"],
            "price": item["price"],
            "category_id": category_id,
            "chipset": clean_basic_info(item.get("chipset")),
            "memory": clean_basic_info(item.get("memory")),
            "battery": clean_basic_info(item.get("battery")),
            "display_resolution": clean_basic_info(item.get("display_resolution")),
            "display_size": clean_basic_info(item.get("display_size")),
            "display_type": clean_basic_info(item.get("display_type")),
            "nfc": has_nfc,
            "storage": clean_basic_info(item.get("storage")),
            "camera_primary": clean_basic_info(item.get("camera_primary")),
            "camera_secondary": clean_basic_info(item.get("camera_secondary")),
            "camera_video": clean_basic_info(item.get("camera_video")),
            "ingest_time": ingest_time,
            "ingest_date": ingest_date,
        }

        # add item data to database
        r = ItemPrice(**data)

        self.session.add(r)
        self.session.commit()
        return item
