import json

import pendulum
from scrapy import Request, Spider
from scrapy.http.response import Response

from common_crawler.spiders.cellphones.constants import (
    BASE_BODY,
    DEFAULT_TZ,
    F_PHONE_CATE_ID,
    PAGE_SIZE,
    PHONE_CATE_ID,
    PHONE_PAGE_LIMIT,
    TABLE_PAGE_LIMIT,
    TABLET_CATE_ID,
)


class CellphonesMobileSpider(Spider):
    name = "cps_mobile"
    custom_settings = {
        "LOG_LEVEL": "INFO",
        "ITEM_PIPELINES": {
            "common_crawler.spiders.cellphones.pipelines.CockroachDBPipeline": 300,
        },
    }

    def start_requests(self):
        # crawl phones
        for i in range(1, PHONE_PAGE_LIMIT + 1):
            body = (
                BASE_BODY.replace("PAGE_INDEX", str(i))
                .replace("PAGE_SIZE", str(PAGE_SIZE))
                .replace("CATEGORY_ID", PHONE_CATE_ID)
            )
            yield Request(
                "https://api.cellphones.com.vn/v2/graphql/query",
                method="POST",
                body=json.dumps({"query": body}),
                headers={"Content-Type": "application/json"},
                callback=self.parse_item_page,
                cb_kwargs={
                    "category_id": F_PHONE_CATE_ID,
                },
            )

        # # crawl tablets
        for i in range(1, TABLE_PAGE_LIMIT + 1):
            body = (
                BASE_BODY.replace("PAGE_INDEX", str(i))
                .replace("PAGE_SIZE", str(PAGE_SIZE))
                .replace("CATEGORY_ID", TABLET_CATE_ID)
            )
            yield Request(
                "https://api.cellphones.com.vn/v2/graphql/query",
                method="POST",
                body=json.dumps({"query": body}),
                headers={"Content-Type": "application/json"},
                callback=self.parse_item_page,
            )

    def parse_item_page(self, response: Response, category_id: int):
        timestamp: pendulum.DateTime = pendulum.now(tz=DEFAULT_TZ)
        data = response.json()

        products = data["data"]["products"]

        if products:
            for i in products:
                try:
                    old_price = i["filterable"]["price"]
                    new_price = i["filterable"]["special_price"]

                    price = old_price
                    if new_price is not None and new_price > 0:
                        price = new_price

                    product_id = i["general"]["product_id"]

                    a = {
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

                    yield a
                except Exception as e:
                    self.logger.error(f"Error parsing item_id {product_id}: {e}")
                    continue
