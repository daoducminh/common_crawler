import json
import os

import pendulum
from scrapy import Request, Spider
from scrapy.http.response import Response

from common_crawler.constants.enums import APP_ENV, TZ_HCM, WARNING_DISCORD_WEBHOOK
from common_crawler.spiders.cellphones.constants import (
    BASE_BODY,
    BASE_HEADERS,
    DEFAULT_TZ,
    F_PHONE_CATE_ID,
    F_TABLET_CATE_ID,
    F_WATCH_CATE_ID,
    PAGE_SIZE,
    PHONE_CATE_ID,
    PHONE_PAGE_LIMIT,
    QUERY_ENDPOINT,
    TABLET_CATE_ID,
    TABLET_PAGE_LIMIT,
    WATCH_CATE_ID,
    WATCH_PAGE_LIMIT,
)
from common_crawler.utils.discord import DiscordNotifier


def get_price(item):
    """Extracts the price from the item."""
    old_price = item["filterable"]["price"]
    new_price = item["filterable"]["special_price"]

    price = old_price
    if new_price is not None and new_price > 0:
        price = new_price

    return price


class CellphonesSpider(Spider):
    name = "cellphones"
    custom_settings = {
        "LOG_LEVEL": "INFO",
        "ITEM_PIPELINES": {
            "common_crawler.spiders.cellphones.pipelines.CockroachDBPipeline": 300,
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.item_count = 0

    async def start(self):
        # crawl phones
        for i in range(1, PHONE_PAGE_LIMIT + 1):
            body = (
                BASE_BODY.replace("PAGE_INDEX", str(i))
                .replace("PAGE_SIZE", str(PAGE_SIZE))
                .replace("CATEGORY_ID", PHONE_CATE_ID)
            )
            yield Request(
                QUERY_ENDPOINT,
                method="POST",
                body=json.dumps({"query": body}),
                headers=BASE_HEADERS,
                callback=self.parse_mobile_item,
                cb_kwargs={
                    "category_id": F_PHONE_CATE_ID,
                },
            )

        # crawl tablets
        for i in range(1, TABLET_PAGE_LIMIT + 1):
            body = (
                BASE_BODY.replace("PAGE_INDEX", str(i))
                .replace("PAGE_SIZE", str(PAGE_SIZE))
                .replace("CATEGORY_ID", TABLET_CATE_ID)
            )
            yield Request(
                QUERY_ENDPOINT,
                method="POST",
                body=json.dumps({"query": body}),
                headers=BASE_HEADERS,
                callback=self.parse_mobile_item,
                cb_kwargs={
                    "category_id": F_TABLET_CATE_ID,
                },
            )

        # crawl watches
        for i in range(1, WATCH_PAGE_LIMIT + 1):
            body = (
                BASE_BODY.replace("PAGE_INDEX", str(i))
                .replace("PAGE_SIZE", str(PAGE_SIZE))
                .replace("CATEGORY_ID", WATCH_CATE_ID)
            )
            yield Request(
                QUERY_ENDPOINT,
                method="POST",
                body=json.dumps({"query": body}),
                headers=BASE_HEADERS,
                callback=self.parse_watch_item,
                cb_kwargs={
                    "category_id": F_WATCH_CATE_ID,
                },
            )

    async def parse_mobile_item(self, response: Response, category_id: int):
        timestamp: pendulum.DateTime = pendulum.now(tz=DEFAULT_TZ)
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")
            return

        products = data["data"]["products"]
        self.item_count += len(products) if products else 0

        if products:
            for i in products:
                product_id = "unknown"
                try:
                    product_id = i["general"]["product_id"]
                    price = get_price(i)

                    item = {
                        "id": product_id,
                        "name": i["general"]["name"],
                        "category_id": category_id,
                        "chipset": i["general"]["attributes"].get("chipset"),
                        "memory": i["general"]["attributes"].get("memory_internal"),
                        "battery": i["general"]["attributes"].get("battery"),
                        "display_resolution": i["general"]["attributes"].get(
                            "display_resolution"
                        ),
                        "display_size": i["general"]["attributes"].get("display_size"),
                        "display_type": i["general"]["attributes"].get(
                            "mobile_type_of_display"
                        ),
                        "nfc": i["general"]["attributes"].get("mobile_nfc"),
                        "storage": i["general"]["attributes"].get("storage"),
                        "camera_primary": i["general"]["attributes"].get(
                            "camera_primary"
                        ),
                        "camera_secondary": i["general"]["attributes"].get(
                            "camera_secondary"
                        ),
                        "camera_video": i["general"]["attributes"].get("camera_video"),
                        "price": price,
                        "ingest_time": timestamp,
                    }

                    yield item
                except Exception as e:
                    self.logger.error(f"Error parsing item_id {product_id}: {e}")
                    continue

    async def parse_watch_item(self, response: Response, category_id: int):
        timestamp: pendulum.DateTime = pendulum.now(tz=DEFAULT_TZ)
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")
            return

        products = data["data"]["products"]
        self.item_count += len(products) if products else 0

        if products:
            for i in products:
                product_id = "unknown"
                try:
                    product_id = i["general"]["product_id"]
                    price = get_price(i)

                    item = {
                        "id": product_id,
                        "name": i["general"]["name"],
                        "category_id": category_id,
                        "battery": i["general"]["attributes"].get("dung_luong_pin"),
                        "display_resolution": i["general"]["attributes"].get(
                            "smart_watch_do_phan_giai"
                        ),
                        "display_size": i["general"]["attributes"].get(
                            "smart_watch_duong_kinh_mat"
                        ),
                        "display_type": i["general"]["attributes"].get("display_type"),
                        "price": price,
                        "ingest_time": timestamp,
                    }

                    yield item
                except Exception as e:
                    self.logger.error(f"Error parsing item_id {product_id}: {e}")
                    continue

    async def closed(self, reason):
        if self.item_count == 0:
            settings = self.settings.copy_to_dict()
            webhook_url = settings.get(WARNING_DISCORD_WEBHOOK)

            if os.getenv(APP_ENV) == "dev":
                webhook_url = os.getenv(WARNING_DISCORD_WEBHOOK) or webhook_url

            if webhook_url:
                self.logger.info("No items extracted, sending Discord notification")
                discord = DiscordNotifier(webhook_url)
                now = pendulum.now(tz=TZ_HCM)
                current_date = now.to_date_string()
                current_time = now.to_time_string()
                await discord.send(
                    f"⚠️ [{current_date}] [{self.name}] finished with 0 items extracted at {current_time}."
                )
            else:
                self.logger.warning(
                    "No items extracted and WARNING_DISCORD_WEBHOOK not configured"
                )
