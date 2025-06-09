PHONE_PAGE_LIMIT = 20
TABLE_PAGE_LIMIT = 10
PAGE_SIZE = 20
PHONE_CATE_ID = "3"
TABLET_CATE_ID = "4"
F_PHONE_CATE_ID = 1
F_TABLET_CATE_ID = 2
F_WATCH_ID = 3

DEFAULT_TZ = "Asia/Ho_Chi_Minh"

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
            attributes
        }
        filterable {
            price
            special_price
        }
    }
}"""
