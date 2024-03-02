from .pc.base_spider import PCBaseSpider


class APPC(PCBaseSpider):
    name = "appc"
    base_url = "https://anphatpc.com.vn/{0}"
    categories = [
        "gaming-laptop.html",
        "may-tinh-xach-tay-laptop.html",
        "apple_dm1064.html",
        "phu-kien-laptop-pc-khac.html",
        "gaming-gear.html",
        "linh-kien-may-tinh.html",
        "pcap-may-tinh-an-phat.html",
        "may-tinh-may-chu.html",
        "man-hinh-may-tinh.html-1",
        "thiet-bi-luu-tru-usb-the-nho.html",
        "camera-quan-sat.html",
        "cooling-tan-nhiet_dm397.html",
        "thiet-bi-mang.html",
    ]
    item_page_css = ".paging a::attr(href)"
    item_cont_css = ".product-list-container .p-item"
    item_id_css = ".p-name::attr(href)"
    item_name_css = ".p-name h3::text"
    item_price_css = ".p-price::text"
    item_category_css = "#breadcrumb ol li:last-child h1::text"
