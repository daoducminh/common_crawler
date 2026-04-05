import json
import os

import pendulum
import pymongo
from scrapy import Request, Spider

from common_crawler.constants.enums import (
    DB_NAME_FINANCE,
    MONGODB_URI,
    GOLD_PRICE_DISCORD_WEBHOOK,
    GASOLINE_PRICE_DISCORD_WEBHOOK,
    TZ_HCM,
    WARNING_DISCORD_WEBHOOK,
)
from common_crawler.utils.discord import DiscordNotifier

# Constants
GOLD_API_URL = (
    "https://priceapi.moneycontrol.com/pricefeed/usMarket/techindicator/D/XAUUSD:CUR"
)
GAS_API_URL = "https://s3.amazonaws.com/oilprice.com/widgets/oilprices/all/last.json"

GOLD_CHANGE_THRESHOLD = 50
GAS_CHANGE_THRESHOLD = 0.2

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

        # Webhooks
        self.gold_webhook_url = settings.get(GOLD_PRICE_DISCORD_WEBHOOK) or os.getenv(
            GOLD_PRICE_DISCORD_WEBHOOK
        )
        self.gas_webhook_url = settings.get(GASOLINE_PRICE_DISCORD_WEBHOOK) or os.getenv(
            GASOLINE_PRICE_DISCORD_WEBHOOK
        )

    def close_spider(self, spider):
        self.client.close()

    async def process_item(self, item, spider):
        # Fetch previous record to detect sentiment change
        prev_item = self.db[COLLECTION_NAME].find_one({"id": item["id"]})

        # Upsert into MongoDB: unique record for "gold" and "gasonline"
        self.db[COLLECTION_NAME].update_one(
            {"id": item["id"]}, {"$set": item}, upsert=True
        )

        reasons = []

        # 1. Price threshold check (Real value)
        change_abs = abs(item.get("change_abs", 0))
        limit = GOLD_CHANGE_THRESHOLD if item["id"] == "gold" else GAS_CHANGE_THRESHOLD

        if change_abs > limit:
            reasons.append(
                f"🚨 Price change ({item.get('change_abs')}) exceeds threshold ({limit})"
            )

        # 2. Gold-specific indicators (RSI, Sentiment, and Technical Changes)
        if item["id"] == "gold":
            indicators = item.get("indicators", {})
            prev_indicators = prev_item.get("indicators", {}) if prev_item else {}

            # RSI Level Check
            rsi_data = indicators.get("RSI(14)", {})
            rsi_val_str = rsi_data.get("value")
            if rsi_val_str:
                try:
                    rsi_val = float(rsi_val_str.replace(",", ""))
                    if rsi_val > 55 or rsi_val < 40:
                        reasons.append(f"⚠️ RSI reaches extreme: {rsi_val}")
                except (ValueError, TypeError):
                    pass

            # Detect breaking change in direction (Sentiment Shift)
            curr_sentiment = item.get("sentiment_indication")
            prev_sentiment = (
                prev_item.get("sentiment_indication") if prev_item else None
            )
            if curr_sentiment and prev_sentiment and curr_sentiment != prev_sentiment:
                reasons.append(
                    f"🔄 Breaking change in direction: {prev_sentiment} ➡️ {curr_sentiment}"
                )

            # Detect individual indicator changes
            for name, data in indicators.items():
                curr_ind = data.get("indication")
                prev_ind = prev_indicators.get(name, {}).get("indication")
                if curr_ind and prev_ind and curr_ind != prev_ind:
                    # Ignore minor changes like Neutral -> Bullish of small indicators if desired,
                    # but for now we highlight all indication changes as they "affect decision"
                    reasons.append(f"⚡ **{name}** indication: {prev_ind} ➡️ {curr_ind}")

            # Detect SMA/EMA/Crossover changes
            for key in ["sma", "ema", "crossovers"]:
                curr_vals = item.get(key, {})
                prev_vals = prev_item.get(key, {}) if prev_item else {}
                for name, curr_ind in curr_vals.items():
                    prev_ind = prev_vals.get(name)
                    if curr_ind and prev_ind and curr_ind != prev_ind:
                        reasons.append(
                            f"📉 **Trend {key.upper()} {name}**: {prev_ind} ➡️ {curr_ind}"
                        )

        await self.send_price_notification(item, reasons)

        return item

    async def send_price_notification(self, item, reasons):
        webhook_url = (
            self.gold_webhook_url if item["id"] == "gold" else self.gas_webhook_url
        )
        if not webhook_url:
            return

        discord = DiscordNotifier(webhook_url)
        emoji = "📈" if item.get("change_abs", 0) > 0 else "📉"

        content = (
            f"{emoji} **Update: {item['id'].capitalize()}**\n"
            f"**Price:** {item['price']}\n"
            f"**Change:** {item.get('change_abs', 'N/A')} ({item.get('change_pct', 0)}%)\n"
            f"**Time:** {item['timestamp'].format('YYYY-MM-DD HH:mm:ss')}\n"
        )

        if reasons:
            content += "\n🔥 **SIGNIFICANT HIGHLIGHTS:**\n"
            for reason in reasons:
                content += f"- {reason}\n"

        if item["id"] == "gold":
            if "sentiment_indication" in item:
                content += f"\n**Overall Sentiment:** {item['sentiment_indication']} ({item.get('total_bullish')} Bullish vs {item.get('total_bearish')} Bearish)\n"

            if "indicators" in item:
                content += "\n**Technical Indicators:**\n"
                for name, data in item["indicators"].items():
                    val = data.get("value")
                    ind = data.get("indication")
                    if isinstance(val, list):
                        val_str = ", ".join(
                            [f"{v['displayName']}: {v['value']}" for v in val]
                        )
                        content += f"- **{name}**: {val_str} ({ind})\n"
                    else:
                        content += f"- **{name}**: {val} ({ind})\n"

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

    async def start(self):
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

            # Extract indicators with indications
            indicators = {}
            for indicator in gold_data.get("indicators", []):
                indicators[indicator["displayName"]] = {
                    "value": indicator["value"],
                    "indication": indicator.get("indication"),
                }

            # Extract SMA/EMA
            sma = {s["key"]: s["indication"] for s in gold_data.get("sma", [])}
            ema = {e["key"]: e["indication"] for e in gold_data.get("ema", [])}

            # Extract Crossovers
            crossovers = {
                c["displayValue"]: c["indication"]
                for c in gold_data.get("crossover", [])
            }

            # Extract sentiments
            sentiments = gold_data.get("sentiments", {})
            sentiment_indication = sentiments.get("indication")
            total_bullish = sentiments.get("totalBullish")
            total_bearish = sentiments.get("totalBearish")

            self.gold_extracted = True
            yield {
                "id": "gold",
                "price": price,
                "change_abs": round(change_abs, 2),
                "change_pct": round(change_pct, 2),
                "indicators": indicators,
                "sma": sma,
                "ema": ema,
                "crossovers": crossovers,
                "sentiment_indication": sentiment_indication,
                "total_bullish": total_bullish,
                "total_bearish": total_bearish,
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
