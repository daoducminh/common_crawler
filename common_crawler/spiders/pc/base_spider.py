import pendulum
from scrapy import Request, Spider
from scrapy.http.response import Response


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

    def start_requests(self):
        for category in self.categories:
            yield Request(self.base_url.format(category), self.parse_product_page)

    def parse_product_page(self, response: Response):
        timestamp: pendulum.DateTime = pendulum.now(tz="Asia/Ho_Chi_Minh")

        for page in response.css(self.item_page_css).getall():
            next_url = response.urljoin(page)
            yield Request(next_url, self.parse_product_page)

        for p in response.css(self.item_cont_css):
            try:
                price_str = p.css(self.item_price_css).get()
                price = int(price_str.replace("đ", "").replace(".", "").strip())

                yield {
                    "id": p.css(self.item_id_css)
                    .get()
                    .split("/")[-1]
                    .replace(".html", ""),
                    "name": p.css(self.item_name_css).get().strip(),
                    "price": price,
                    "source": self.name,
                    "category": response.css(self.item_category_css).get().strip(),
                    "timestamp": timestamp,
                    "ingest_date": timestamp.date(),
                }
            except ValueError:
                pass
            except Exception as e:
                self.logger.error(e)
