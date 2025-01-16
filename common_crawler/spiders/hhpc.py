from .pc.base_spider import PCBaseSpider


class HHPC(PCBaseSpider):
    name = "hhpc"
    base_url = "https://hoanghapc.vn/{0}"
    categories = [
        "pc-workstation",
        "hhpc-workstation-render-edit-video",
        "pc-dep",
        "pc-gaming",
        "machine-learning-ai-tensorflow",
        "linh-kien-may-tinh",
        "may-tinh-van-phong",
        "hdd-ssd-nas",
        "tan-nhiet-cooling",
        "thiet-bi-mang",
        "gaming-gear",
        "man-hinh-may-tinh",
    ]
    item_page_css = ".paging a::attr(href)"
    item_cont_css = ".p-container .p-item"
    item_id_css = ".p-name::attr(href)"
    item_name_css = ".p-name h3::text"
    item_price_css = ".p-price::text"
    item_category_css = ".page-title::text"
