import logging
import os

import pendulum
import pymongo
from dotenv import load_dotenv
from itemloaders import ItemLoader
from itemloaders.processors import Identity, Join, MapCompose, TakeFirst
from scrapy import Field, Item, Request, Spider
from scrapy.http.response import Response

from common_crawler.utils.discord import DiscordWebhook

load_dotenv()


ACTIVE_BOX = "section[contains(@class,'wds-tabber')]/div[@class='wds-tab__content wds-is-current']/section"


def extract_internal_id(text: str) -> str:
    return text.split(" ")[0].strip()


class CoreKeeperItem(Item):
    name = Field(input_processor=MapCompose(str.strip), output_processor=TakeFirst())
    url = Field(output_processor=TakeFirst())
    type = Field(
        input_processor=MapCompose(str.strip),
        output_processor=Identity(),
    )
    category = Field(
        input_processor=MapCompose(str.strip),
        output_processor=Identity(),
    )
    rarity = Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst(),
    )
    level = Field(
        input_processor=MapCompose(str.strip, int),
        output_processor=TakeFirst(),
    )
    slot = Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst(),
    )
    durability = Field(
        input_processor=MapCompose(str.strip, int),
        output_processor=TakeFirst(),
    )
    effects = Field(
        input_processor=MapCompose(str.strip),
        output_processor=Identity(),
    )
    tooltip = Field(
        input_processor=MapCompose(str.strip),
        output_processor=Join(),
    )
    sell_price = Field(
        input_processor=MapCompose(str.strip, int),
        output_processor=TakeFirst(),
    )
    internal_id = Field(
        input_processor=MapCompose(extract_internal_id),
        output_processor=TakeFirst(),
    )
    code = Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst(),
    )


class CoreKeeperPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings) -> None:
        self.collection_name = "item"
        self.mongo_uri = settings.get("MONGODB_URI") or os.getenv("MONGODB_URI")
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client["core_keeper"]

    def open_spider(self):
        pass

    def close_spider(self):
        self.client.close()

    def process_item(self, item: CoreKeeperItem, spider: Spider):
        if item.get("type"):
            self.db[self.collection_name].update_one(
                {"name": item["name"]},
                {"$set": dict(item)},
                upsert=True,
            )

        return item


class CoreKeeperSpider(Spider):
    name = "core_keeper"
    allowed_domains = ["core-keeper.fandom.com"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.item_count = 0

    async def start(self):
        yield Request(
            "https://core-keeper.fandom.com/wiki/Core_Keeper_Wiki", callback=self.parse
        )

    custom_settings = {
        "LOG_LEVEL": logging.INFO,
        "LOG_FILE": "logs/core_keeper.log",
        # "CONCURRENT_REQUESTS": 4,
        "ITEM_PIPELINES": {
            "common_crawler.spiders.core_keeper.CoreKeeperPipeline": 300,
        },
    }

    async def parse(self, response: Response):
        links = response.css('a[href^="/wiki"]::attr(href)').getall()

        for link in links:
            clean_path = link

            try:
                clean_path = link[: link.index("?")]
            except Exception as e:
                self.logger.error(e)

            yield Request(response.urljoin(clean_path), callback=self.parse)

        info_boxes = response.css(".portable-infobox")

        for info_box in info_boxes:
            item_with_level = info_box.xpath(ACTIVE_BOX)
            prefix_path = "figure/following-sibling::section"

            if item_with_level:
                prefix_path = ACTIVE_BOX

            loader = ItemLoader(item=CoreKeeperItem(), selector=info_box)

            loader.add_css("name", ".pi-title::text")
            loader.add_value("url", response.url)
            loader.add_xpath(
                "type",
                prefix_path + "//*[text()='Type']/following-sibling::div//li/a/text()",
            )
            loader.add_xpath(
                "category",
                prefix_path
                + "//*[text()='Category']/following-sibling::div//li/text()",
            )
            loader.add_xpath(
                "rarity",
                prefix_path + "//*[text()='Rarity']/following-sibling::div/text()",
            )
            loader.add_xpath(
                "level",
                prefix_path + "//*[text()='Level']/following-sibling::div/text()",
            )
            loader.add_xpath(
                "slot",
                prefix_path + "//*[text()='Slot']/following-sibling::div/text()",
            )
            loader.add_xpath(
                "durability",
                prefix_path + "//*[text()='Durability']/following-sibling::div/text()",
            )
            loader.add_xpath(
                "effects",
                prefix_path + "//*[text()='Effects']/following-sibling::div/text()",
            )
            loader.add_xpath(
                "tooltip",
                prefix_path + "//*[text()='Tooltip']/following-sibling::div/text()",
            )
            loader.add_xpath(
                "sell_price",
                prefix_path + "//*[text()='Sell']/following-sibling::div/span/text()",
            )
            loader.add_xpath(
                "internal_id",
                ".//h3[text()='Internal ID']/following-sibling::div//ul/li[1]/text()",
            )
            loader.add_xpath(
                "code",
                ".//h3[text()='Internal ID']/following-sibling::div//ul/li[2]/code/text()",
            )

            item = loader.load_item()
            if item.get("name"):
                self.item_count += 1
            yield item

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
