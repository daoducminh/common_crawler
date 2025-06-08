import json

import pendulum
from scrapy import Request, Spider
from scrapy.http.response import Response
from datetime import date

import pendulum
from sqlalchemy import TIMESTAMP, Date, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


PHONE_PAGE_LIMIT = 1
TABLE_PAGE_LIMIT = 10
PAGE_SIZE = 20
PHONE_CATE_ID = "3"
TABLET_CATE_ID = "4"

BASE_BODY = """query GetProductsByCateId {
    products(
        filter: {
            static: {
                categories: ["CATEGORY_ID"]
                province_id: 24
                stock: { from: 0 }
                stock_available_id: [46, 4920]
                filter_price: { from: 0, to: 54990000 }
            }
            dynamic: {  }
        }
        page: PAGE_INDEX
        size: PAGE_SIZE
        sort: [{ view: desc }]
    ) {
        general {
            product_id
            name
        }
        filterable {
            price
            special_price
        }
    }
}"""


class Base(DeclarativeBase):
    pass


class CellphonesSpider(Spider):
    name = "cellphones"

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
            )

        # # crawl tablets
        # for i in range(1, TABLE_PAGE_LIMIT + 1):
        #     body = (
        #         BASE_BODY.replace("PAGE_INDEX", str(i))
        #         .replace("PAGE_SIZE", str(PAGE_SIZE))
        #         .replace("CATEGORY_ID", TABLET_CATE_ID)
        #     )
        #     yield Request(
        #         "https://api.cellphones.com.vn/v2/graphql/query",
        #         method="POST",
        #         body=json.dumps({"query": body}),
        #         headers={"Content-Type": "application/json"},
        #         callback=self.parse_item_page,
        #     )

    def parse_item_page(self, response: Response):
        timestamp: pendulum.DateTime = pendulum.now(tz="Asia/Ho_Chi_Minh")
        data = response.json()
