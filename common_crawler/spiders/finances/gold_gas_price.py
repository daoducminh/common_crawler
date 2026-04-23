import json
import os
import re
from dataclasses import dataclass
from typing import List, Optional

import pendulum
import pymongo
from dotenv import load_dotenv
from scrapy import Request, Spider

from common_crawler.constants.enums import (
    DB_NAME_FINANCE,
    GOLD_PRICE_DISCORD_WEBHOOK,
    MONGODB_URI,
    TZ_HCM,
    WARNING_DISCORD_WEBHOOK,
)
from common_crawler.utils.discord import DiscordNotifier

load_dotenv()


@dataclass
class FinanceConfig:
    """Configuration for finance data extraction and processing."""

    # API URLs
    gold_api_url: str = "https://priceapi.moneycontrol.com/pricefeed/usMarket/techindicator/D/XAUUSD:CUR"
    gas_api_url: str = (
        "https://s3.amazonaws.com/oilprice.com/widgets/oilprices/all/last.json"
    )
    gold_btmc_url: str = "https://giavang.org/trong-nuoc/bao-tin-minh-chau/"
    gas_vn_url: str = "https://giaxanghomnay.com/"

    # Thresholds
    gold_change_threshold: float = 50
    gas_change_threshold: float = 0.2

    # Database
    db_name: str = DB_NAME_FINANCE
    collection_name: str = "prices"

    # Record IDs
    gold_price_record_id: str = "prices.gold"
    gas_price_record_id: str = "prices.gasoline"
    gold_btmc_price_record_id: str = "prices.gold.btmc"
    gas_vn_price_record_id: str = "prices.gasoline.vn"

    # Discord message tracking
    gold_discord_last_msg_id: str = "discord.gold.last_msg_id"
    gas_discord_last_msg_id: str = "discord.gasoline.last_msg_id"
    gold_btmc_discord_last_msg_id: str = "discord.gold.btmc.last_msg_id"
    gas_vn_discord_last_msg_id: str = "discord.gasoline.vn.last_msg_id"

    # BTMC target gold types
    target_btmc_gold_types: List[str] = None

    def __post_init__(self):
        if self.target_btmc_gold_types is None:
            self.target_btmc_gold_types = ["Giá vàng Miếng", "Giá vàng Nhẫn"]


config = FinanceConfig()


class PriceChangeDetector:
    """Helper class to detect price changes."""

    @staticmethod
    def detect_price_change(curr, prev, threshold: float) -> bool:
        """Detect if price change exceeds threshold."""
        curr_change_abs = curr.get("change_abs", 0)
        prev_change_abs = prev.get("change_abs", 0) if prev else 0
        curr_price = curr.get("price", 0)
        prev_price = prev.get("price", 0) if prev else 0

        if abs(curr_change_abs) > threshold:
            if abs(prev_change_abs) <= threshold:
                return True
            elif abs(curr_price - prev_price) >= threshold:
                return True
        return False

    @staticmethod
    def detect_btmc_price_change(
        curr_prices, prev_prices, target_types: List[str]
    ) -> tuple:
        """Detect BTMC price changes and return indicators."""
        has_change = False
        price_changes = {}

        for name in target_types:
            curr = curr_prices.get(name)
            prev = prev_prices.get(name)

            if not prev or not curr:
                continue

            buy_indicator = ""
            sell_indicator = ""

            try:
                if curr["buy"] > prev["buy"]:
                    buy_indicator = "🟢 🔺"
                    has_change = True
                elif curr["buy"] < prev["buy"]:
                    buy_indicator = "🔴 🔻"
                    has_change = True

                if curr["sell"] > prev["sell"]:
                    sell_indicator = "🟢 🔺"
                    has_change = True
                elif curr["sell"] < prev["sell"]:
                    sell_indicator = "🔴 🔻"
                    has_change = True
            except (ValueError, TypeError):
                continue

            price_changes[name] = {"buy": buy_indicator, "sell": sell_indicator}

        return has_change, price_changes


class NotificationFormatter:
    """Helper class to format notification content."""

    @staticmethod
    def format_btmc_notification(item, price_changes) -> str:
        """Format BTMC gold price notification content."""
        content = "🔔 **Cập Nhật Giá Vàng Bảo Tín Minh Châu**\n"
        content += f"**Thời gian:** {item['timestamp'].format('YYYY-MM-DD HH:mm:ss')}\n"

        content += "\n**Giá hiện tại:**\n"
        for name in config.target_btmc_gold_types:
            p = item["prices"].get(name)
            if p:
                indicators = price_changes.get(name, {"buy": "", "sell": ""})
                content += f"🔸 **{name}**\n"
                content += f"  - Mua vào: `{p['buy']}` {indicators['buy']}\n"
                content += f"  - Bán ra: `{p['sell']}` {indicators['sell']}\n"
                content += f"  - Cập nhật lúc: {p['time']}\n"

        return content

    @staticmethod
    def format_gas_vn_notification(item, price_change: dict) -> str:
        """Format gas VN price notification content."""
        content = "🔔 **Cập Nhật Giá Xăng Việt Nam (Petrolimex)**\n"
        content += f"**Thời gian:** {item['timestamp'].format('YYYY-MM-DD HH:mm:ss')}\n"

        content += "\n**Giá hiện tại:**\n"
        content += "🔸 **Xăng RON 95-V**\n"
        content += (
            f"  - Giá vùng 1: `{item['price']}` {price_change.get('indicator', '')}\n"
        )

        return content

    @staticmethod
    def format_price_notification(item, reasons) -> str:
        """Format generic price notification content."""
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

        if item["id"] == config.gold_price_record_id:
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

        return content


class DiscordMessageManager:
    """Helper class to manage Discord message operations."""

    def __init__(self, db, webhook_url: str):
        self.db = db
        self.webhook_url = webhook_url

    async def delete_old_message(self, msg_record_id: str):
        """Delete old Discord message if exists."""
        last_msg_doc = self.db[config.collection_name].find_one({"id": msg_record_id})
        if last_msg_doc:
            old_msg_id = last_msg_doc.get("last_msg_id")
            if old_msg_id:
                try:
                    discord = DiscordNotifier(self.webhook_url)
                    await discord.delete(old_msg_id)
                except Exception:
                    pass

    async def send_and_store_message(
        self, content: str, msg_record_id: str
    ) -> Optional[str]:
        """Send Discord message and store its ID."""
        if not self.webhook_url:
            return None

        discord = DiscordNotifier(self.webhook_url)
        msg_id = await discord.send(content)

        if msg_id:
            self.db[config.collection_name].update_one(
                {"id": msg_record_id},
                {"$set": {"last_msg_id": msg_id}},
                upsert=True,
            )

        return msg_id


class FinancePipeline:
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        pipeline.crawler = crawler
        return pipeline

    def __init__(self, settings) -> None:
        self.mongo_uri = settings.get(MONGODB_URI) or os.getenv(MONGODB_URI)
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[config.db_name]

        # Webhooks
        self.gold_webhook_url = settings.get(GOLD_PRICE_DISCORD_WEBHOOK) or os.getenv(
            GOLD_PRICE_DISCORD_WEBHOOK
        )

        # Discord message manager
        self.discord_manager = DiscordMessageManager(self.db, self.gold_webhook_url)

    def close_spider(self):
        self.client.close()

    async def process_item(self, item):
        # Fetch previous record to detect sentiment change
        prev_item = self.db[config.collection_name].find_one({"id": item["id"]})

        # Upsert into MongoDB: unique record for "gold" and "gasonline"
        self.db[config.collection_name].update_one(
            {"id": item["id"]}, {"$set": item}, upsert=True
        )

        # Handle BTMC items separately
        if item["id"] == config.gold_btmc_price_record_id:
            return await self.process_btmc_item(item, prev_item)

        # Handle gas_vn items separately
        if item["id"] == config.gas_vn_price_record_id:
            return await self.process_gas_vn_item(item, prev_item)

        reasons = []

        # 1. Price threshold check (Stateful)
        limit = (
            config.gold_change_threshold
            if item["id"] == config.gold_price_record_id
            else config.gas_change_threshold
        )

        is_breaking_price = PriceChangeDetector.detect_price_change(
            item, prev_item, limit
        )

        if is_breaking_price:
            reasons.append(
                f"🚨 Significant price change ({item.get('change_abs', 0)}) exceeds threshold ({limit})"
            )

        # 2. Gold-specific indicators (RSI, Sentiment, and Technical Changes)
        if item["id"] == config.gold_price_record_id:
            indicators = item.get("indicators", {})
            prev_indicators = prev_item.get("indicators", {}) if prev_item else {}

            # RSI Level Check (Stateful)
            rsi_data = indicators.get("RSI(14)", {})
            rsi_val_str = rsi_data.get("value")
            if rsi_val_str:
                try:
                    rsi_val = float(rsi_val_str.replace(",", ""))
                    prev_rsi_val_str = prev_indicators.get("RSI(14)", {}).get("value")
                    prev_rsi_val = (
                        float(prev_rsi_val_str.replace(",", ""))
                        if prev_rsi_val_str
                        else None
                    )

                    is_extreme = rsi_val > 55 or rsi_val < 40
                    prev_is_extreme = prev_rsi_val is not None and (
                        prev_rsi_val > 55 or prev_rsi_val < 40
                    )

                    if is_extreme and not prev_is_extreme:
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

        if reasons:
            await self.send_price_notification(item, reasons)

        return item

    async def process_btmc_item(self, item, prev_item):
        if not prev_item:
            # First run, don't send notification
            return item

        prev_prices = prev_item.get("prices", {})
        curr_prices = item.get("prices", {})

        has_change, price_changes = PriceChangeDetector.detect_btmc_price_change(
            curr_prices, prev_prices, config.target_btmc_gold_types
        )

        if has_change:
            await self.send_btmc_notification(item, price_changes)

        return item

    async def process_gas_vn_item(self, item, prev_item):
        if not prev_item:
            # First run, don't send notification
            return item

        prev_price = prev_item.get("price", 0)
        curr_price = item.get("price", 0)

        price_change = {"indicator": ""}
        has_change = False

        try:
            if curr_price > prev_price:
                price_change["indicator"] = "🟢 🔺"
                has_change = True
            elif curr_price < prev_price:
                price_change["indicator"] = "🔴 🔻"
                has_change = True
        except (ValueError, TypeError):
            pass

        if has_change:
            await self.send_gas_vn_notification(item, price_change)

        return item

    async def send_btmc_notification(self, item, price_changes):
        if not self.gold_webhook_url:
            return

        # Delete old message
        await self.discord_manager.delete_old_message(
            config.gold_btmc_discord_last_msg_id
        )

        # Format and send content
        content = NotificationFormatter.format_btmc_notification(item, price_changes)
        await self.discord_manager.send_and_store_message(
            content, config.gold_btmc_discord_last_msg_id
        )

    async def send_gas_vn_notification(self, item, price_change):
        if not self.gold_webhook_url:
            return

        # Delete old message
        await self.discord_manager.delete_old_message(config.gas_vn_discord_last_msg_id)

        # Format and send content
        content = NotificationFormatter.format_gas_vn_notification(item, price_change)
        await self.discord_manager.send_and_store_message(
            content, config.gas_vn_discord_last_msg_id
        )

    async def send_price_notification(self, item, reasons):
        if not self.gold_webhook_url:
            return

        # Determine message record ID
        msg_record_id = (
            config.gold_discord_last_msg_id
            if item["id"] == config.gold_price_record_id
            else config.gas_discord_last_msg_id
        )

        # Delete old message
        await self.discord_manager.delete_old_message(msg_record_id)

        # Format and send content
        content = NotificationFormatter.format_price_notification(item, reasons)
        await self.discord_manager.send_and_store_message(content, msg_record_id)


class BTMCPriceExtractor:
    """Helper class to extract BTMC gold prices from HTML."""

    @staticmethod
    def extract_price_from_text(text: str, pattern: str) -> int:
        """Extract and convert price from text using regex pattern."""
        try:
            match = re.search(pattern, text)
            if match:
                return int(match.group(1).replace(".", "").replace(",", ""))
        except (ValueError, AttributeError):
            pass
        return 0

    @staticmethod
    def extract_heading_prices(response, target_types: List[str]) -> dict:
        """Extract prices from h2 headings matching target types."""
        extracted_prices = {}
        headings = response.css("h2")

        for heading in headings:
            heading_text = heading.css("::text").get("")
            if not heading_text:
                continue

            heading_text = heading_text.strip()

            # Check if this heading matches any of our target gold types
            matched_type = None
            for target_type in target_types:
                if target_type in heading_text:
                    matched_type = target_type
                    break

            if matched_type:
                # Get the parent element to extract prices
                parent = heading.xpath("..")
                if not parent:
                    continue

                parent_text = parent[0].css("::text").getall()
                parent_text = " ".join([t.strip() for t in parent_text if t.strip()])

                # Extract buy and sell prices
                buy = BTMCPriceExtractor.extract_price_from_text(
                    parent_text, r"Mua vào\s*([\d.,]+)"
                )
                sell = BTMCPriceExtractor.extract_price_from_text(
                    parent_text, r"Bán ra\s*([\d.,]+)"
                )

                # Extract update time from page
                time_match = re.search(
                    r"Cập nhật lúc\s*([\d:]+\s*[\d/]+)", response.text
                )
                update_time = (
                    time_match.group(1)
                    if time_match
                    else pendulum.now(tz=TZ_HCM).format("HH:mm DD/MM/YYYY")
                )

                extracted_prices[matched_type] = {
                    "buy": buy,
                    "sell": sell,
                    "time": update_time,
                }

        return extracted_prices


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
        self.btmc_extracted = False
        self.gas_vn_extracted = False

    async def start(self):
        yield Request(config.gold_api_url, callback=self.parse_gold)
        yield Request(config.gas_api_url, callback=self.parse_gas)
        yield Request(config.gold_btmc_url, callback=self.parse_btmc)
        yield Request(
            config.gas_vn_url,
            callback=self.parse_gas_vn,
            meta={"dont_merge_cookies": False},
            headers={"User-Agent": "Mozilla/5.0 (compatible; Scrapy/2.14)"},
        )

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
                "id": config.gold_price_record_id,
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
                "id": config.gas_price_record_id,
                "price": price,
                "change_abs": change_abs,
                "change_pct": change_pct,
                "timestamp": pendulum.now(tz=TZ_HCM),
            }
        except Exception as e:
            self.logger.error(f"Error parsing gas API: {e}")

    async def parse_btmc(self, response):
        try:
            extracted_prices = BTMCPriceExtractor.extract_heading_prices(
                response, config.target_btmc_gold_types
            )

            if extracted_prices:
                self.btmc_extracted = True
                yield {
                    "id": config.gold_btmc_price_record_id,
                    "prices": extracted_prices,
                    "timestamp": pendulum.now(tz=TZ_HCM),
                }
            else:
                self.logger.warning("No target BTMC gold prices found")
        except Exception as e:
            self.logger.error(f"Error parsing BTMC page: {e}")

    async def parse_gas_vn(self, response):
        try:
            # Use regex to find the price directly in the HTML
            # Look for pattern: Xăng RON 95-V followed by numbers in the price column
            text = response.text

            # Try to find the pattern for Xăng RON 95-V row
            # Pattern matches: Xăng RON 95-V, then some content, then the price (23,820 or similar)
            pattern = (
                r"Xăng RON 95-V[^>]*>[^<]*<[^>]*>[^<]*<[^>]*>[^<]*<[^>]*>([\d.,]+)"
            )
            match = re.search(pattern, text)

            if match:
                price_text = match.group(1)
                try:
                    price = int(price_text.replace(".", "").replace(",", ""))
                    self.gas_vn_extracted = True
                    yield {
                        "id": config.gas_vn_price_record_id,
                        "price": price,
                        "timestamp": pendulum.now(tz=TZ_HCM),
                    }
                    return
                except (ValueError, TypeError):
                    self.logger.error(f"Could not convert price to int: {price_text}")

            # Fallback: try table-based extraction
            tables = response.css("table")
            price = None
            if tables:
                table = tables[0]
                rows = table.css("tr")

                for row in rows:
                    row_text = row.css("::text").getall()
                    row_text = " ".join([t.strip() for t in row_text if t.strip()])

                    if "Xăng RON 95-V" in row_text:
                        # Extract price using regex from row text
                        price_match = re.search(
                            r"Xăng RON 95-V.*?(\d{1,3}[.,]\d{3})", row_text
                        )
                        if price_match:
                            try:
                                price = int(
                                    price_match.group(1)
                                    .replace(".", "")
                                    .replace(",", "")
                                )
                                break
                            except (ValueError, TypeError):
                                pass

            if price:
                self.gas_vn_extracted = True
                yield {
                    "id": config.gas_vn_price_record_id,
                    "price": price,
                    "timestamp": pendulum.now(tz=TZ_HCM),
                }
            else:
                self.logger.warning(
                    "Could not find Xăng RON 95-V price in Petrolimex section"
                )
        except Exception as e:
            self.logger.error(f"Error parsing gas VN page: {e}")

    async def closed(self, reason):
        if (
            not self.gold_extracted
            or not self.gas_extracted
            or not self.btmc_extracted
            or not self.gas_vn_extracted
        ):
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
                if not self.btmc_extracted:
                    failed.append("BTMC Gold")
                if not self.gas_vn_extracted:
                    failed.append("Gasoline VN")

                now = pendulum.now(tz=TZ_HCM)
                await discord.send(
                    f"⚠️ **Extraction Warning: {self.name}**\n"
                    f"Failed to extract: {', '.join(failed)}\n"
                    f"Time: {now.to_datetime_string()}"
                )
