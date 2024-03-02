import pendulum
import os
from scrapy import Request, Spider
from scrapy.http.response import Response
from common_crawler.utils.discord import DiscordWebhook


class PCBaseSpider(Spider):
    custom_settings = {
        "LOG_LEVEL": "INFO",
        "ITEM_PIPELINES": {
            "common_crawler.spiders.pc.pipeline.CockroachDBPipeline": 300,
        },
    }

    name: str
    base_url: str
    categories: list[str]
    item_page_css: str
    item_cont_css: str
    item_id_css: str
    item_name_css: str
    item_price_css: str
    item_category_css: str

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.item_count = 0

    async def start(self):
        for category in self.categories:
            yield Request(self.base_url.format(category), self.parse_product_page)

    async def parse_product_page(self, response: Response):
        timestamp: pendulum.DateTime = pendulum.now(tz="Asia/Ho_Chi_Minh")

        for page in response.css(self.item_page_css).getall():
            next_url = response.urljoin(page)
            yield Request(next_url, self.parse_product_page)

        for p in response.css(self.item_cont_css):
            try:
                price_str = p.css(self.item_price_css).get()

                if price_str is None:
                    continue
                price = int(price_str.replace("đ", "").replace(".", "").strip())

                id_str = p.css(self.item_id_css).get()
                if id_str is None:
                    continue
                id = id_str.split("/")[-1].replace(".html", "")

                name_str = p.css(self.item_name_css).get()
                if name_str is None:
                    continue
                name = name_str.strip()

                category_str = response.css(self.item_category_css).get()
                if category_str is None:
                    continue
                category = category_str.strip()

                item_data = {
                    "id": id,
                    "name": name,
                    "price": price,
                    "source": self.name,
                    "category": category,
                    "timestamp": timestamp,
                    "ingest_date": timestamp.date(),
                }
                self.item_count += 1
                yield item_data
            except ValueError:
                continue
            except Exception as e:
                self.logger.error(e)

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
