import json
import os

import pendulum
import pymongo
from scrapy import Request, Spider

from common_crawler.constants.enums import (
    DB_NAME_FINANCE,
    MONGODB_URI,
    PRICE_DISCORD_WEBHOOK,
    TZ_HCM,
    WARNING_DISCORD_WEBHOOK,
)
from common_crawler.utils.discord import DiscordNotifier

# Constants
GOLD_API_URL = (
    "https://priceapi.moneycontrol.com/pricefeed/usMarket/techindicator/D/XAUUSD:CUR"
)
GAS_API_URL = "https://s3.amazonaws.com/oilprice.com/widgets/oilprices/all/last.json"

GOLD_CHANGE_THRESHOLD = 0.05
GAS_CHANGE_THRESHOLD = 0.03

DB_NAME = DB_NAME_FINANCE
COLLECTION_NAME = "prices"


class FinancePipeline:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings) -> None:
        self.mongo_uri = settings.get(MONGODB_URI) or os.getenv(MONGODB_URI)
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[DB_NAME]

        # Webhook for price alerts (threshold exceeded)
        self.price_webhook_url = settings.get(PRICE_DISCORD_WEBHOOK) or os.getenv(
            PRICE_DISCORD_WEBHOOK
        )

    def close_spider(self, spider):
        self.client.close()

    async def process_item(self, item, spider):
        # Upsert into MongoDB: unique record for "gold" and "gasoline"
        self.db[COLLECTION_NAME].update_one(
            {"id": item["id"]}, {"$set": item}, upsert=True
        )

        # Check threshold for price change
        change_pct = abs(item.get("change_pct", 0))
        limit = (
            GOLD_CHANGE_THRESHOLD if item["id"] == "gold" else GAS_CHANGE_THRESHOLD
        ) * 100

        if change_pct > limit:
            await self.send_price_notification(item)

        return item

    async def send_price_notification(self, item):
        if not self.price_webhook_url:
            return

        discord = DiscordNotifier(self.price_webhook_url)
        emoji = "📈" if item.get("change_pct", 0) > 0 else "📉"

        content = (
            f"{emoji} **Price Alert: {item['id'].capitalize()}**\n"
            f"**Price:** {item['price']}\n"
            f"**Change:** {item.get('change_abs', 'N/A')} ({item.get('change_pct', 0)}%)\n"
            f"**Time:** {item['timestamp'].to_datetime_string()}\n"
        )

        if item["id"] == "gold" and "indicators" in item:
            content += "\n**Technical Indicators:**\n"
            for name, val in item["indicators"].items():
                if isinstance(
                    val, list
                ):  # Handle complex indicators like Bollinger Band
                    val_str = ", ".join(
                        [f"{v['displayName']}: {v['value']}" for v in val]
                    )
                    content += f"- **{name}**: {val_str}\n"
                else:
                    content += f"- **{name}**: {val}\n"

        await discord.send(content)


class GoldGasPriceSpider(Spider):
    name = "gold_gas_price"

    custom_settings = {
        "ITEM_PIPELINES": {
            "common_crawler.spiders.finances.gold_gas_price.FinancePipeline": 300,
        },
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gold_extracted = False
        self.gas_extracted = False

    def start_requests(self):
        yield Request(GOLD_API_URL, callback=self.parse_gold)
        yield Request(GAS_API_URL, callback=self.parse_gas)

    async def parse_gold(self, response):
        try:
            data = json.loads(response.text)
            if data.get("code") != "200" or "data" not in data:
                self.logger.error("Invalid response from Gold API")
                return

            gold_data = data["data"]
            price = gold_data.get("close")
            pclose = gold_data.get("pclose")

            if price is None:
                self.logger.error("Could not find gold price in API response")
                return

            change_abs = price - pclose if pclose else 0
            change_pct = (change_abs / pclose * 100) if pclose else 0

            # Extract indicators
            indicators = {}
            for indicator in gold_data.get("indicators", []):
                indicators[indicator["displayName"]] = indicator["value"]

            self.gold_extracted = True
            yield {
                "id": "gold",
                "price": price,
                "change_abs": round(change_abs, 2),
                "change_pct": round(change_pct, 2),
                "indicators": indicators,
                "timestamp": pendulum.now(tz=TZ_HCM),
            }
        except Exception as e:
            self.logger.error(f"Error parsing gold API: {e}")

    async def parse_gas(self, response):
        try:
            data = json.loads(response.text)
            # Gas ID is 53 based on previous analysis
            gas_data = data.get("53")
            if not gas_data:
                self.logger.error("Could not find gasoline data in API response")
                return

            price = gas_data.get("price")
            change_abs = gas_data.get("change")
            change_pct = gas_data.get("change_percent")

            self.gas_extracted = True
            yield {
                "id": "gasoline",
                "price": price,
                "change_abs": change_abs,
                "change_pct": change_pct,
                "timestamp": pendulum.now(tz=TZ_HCM),
            }
        except Exception as e:
            self.logger.error(f"Error parsing gas API: {e}")

    async def closed(self, reason):
        if not self.gold_extracted or not self.gas_extracted:
            webhook_url = self.settings.get(WARNING_DISCORD_WEBHOOK) or os.getenv(
                WARNING_DISCORD_WEBHOOK
            )
            if webhook_url:
                discord = DiscordNotifier(webhook_url)
                failed = []
                if not self.gold_extracted:
                    failed.append("Gold")
                if not self.gas_extracted:
                    failed.append("Gasoline")

                now = pendulum.now(tz=TZ_HCM)
                await discord.send(
                    f"⚠️ **Extraction Warning: {self.name}**\n"
                    f"Failed to extract: {', '.join(failed)}\n"
                    f"Time: {now.to_datetime_string()}"
                )
