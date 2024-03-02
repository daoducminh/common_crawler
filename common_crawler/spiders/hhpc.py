import json
import os

import pendulum
from scrapy import Request, Spider
from scrapy.http.response import Response

from common_crawler.utils.discord import DiscordWebhook

CATEGORIES = ["1", "284", "27", "93", "168", "2", "3", "6", "166", "5"]
BASE_URL = "https://hoanghapc.vn/ajax/get_json.php?action=product&action_type=product-list&category={category_id}&sort=order&show={show}&page={page}"


class HHPCSpider(Spider):
    name = "hhpc"

    custom_settings = {
        "LOG_LEVEL": "INFO",
        "ITEM_PIPELINES": {
            "common_crawler.spiders.pc.pipeline.CockroachDBPipeline": 300,
        },
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 2,
    }

    def __init__(self, token=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.items_per_page = 30
        self.headers = {"authorization": f"Basic {self.token}" if self.token else ""}
        self.item_count = 0

    async def start(self):
        if not self.token:
            self.logger.warning("No token provided. Please provide via '-a token=...'")

        for category_id in CATEGORIES:
            url = BASE_URL.format(
                category_id=category_id, show=self.items_per_page, page=1
            )
            yield Request(
                url,
                headers=self.headers,
                callback=self.parse_page,
                meta={"category_id": category_id, "page": 1},
            )

    async def parse_page(self, response: Response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")
            return

        category_id = response.meta.get("category_id")
        current_page = response.meta.get("page")
        timestamp = pendulum.now(tz="Asia/Ho_Chi_Minh")

        items = data.get("list", [])
        self.item_count += len(items)
        for item in items:
            product_id = item.get("productId")
            if not product_id:
                continue

            yield {
                "id": str(product_id),
                "name": item.get("productName"),
                "price": item.get("price"),
                "source": self.name,
                "category": str(category_id),
                "timestamp": timestamp,
                "ingest_date": timestamp.date(),
            }

        # If it's the first page, calculate total pages and yield requests for the rest
        if current_page == 1:
            total = data.get("total", 0)
            total_pages = (total + self.items_per_page - 1) // self.items_per_page

            for p in range(2, total_pages + 1):
                url = BASE_URL.format(
                    category_id=category_id, show=self.items_per_page, page=p
                )
                yield Request(
                    url,
                    headers=self.headers,
                    callback=self.parse_page,
                    meta={"category_id": category_id, "page": p},
                )

    async def closed(self, reason):
        if self.item_count == 0:
            settings = self.settings.copy_to_dict()
            webhook_url = settings.get("DISCORD_WEBHOOK_URL")

            if os.getenv("ENV") == "dev":
                webhook_url = os.getenv("DISCORD_WEBHOOK_URL") or webhook_url

            if webhook_url:
                self.logger.info("No items extracted, sending Discord notification")
                discord = DiscordWebhook(webhook_url)
                now = pendulum.now(tz="Asia/Ho_Chi_Minh")
                current_date = now.to_date_string()
                current_time = now.to_time_string()
                await discord.send(
                    f"⚠️ [{current_date}] [{self.name}] finished with 0 items extracted at {current_time}."
                )
            else:
                self.logger.warning(
                    "No items extracted and DISCORD_WEBHOOK_URL not configured"
                )
