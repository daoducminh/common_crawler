# Phones
PHONE_CATE_ID = "3"
F_PHONE_CATE_ID = 1
PHONE_PAGE_LIMIT = 20

# Tablets
TABLET_CATE_ID = "4"
F_TABLET_CATE_ID = 2
TABLET_PAGE_LIMIT = 10

# Watches
WATCH_CATE_ID = "610"
F_WATCH_CATE_ID = 3
WATCH_PAGE_LIMIT = 10

# Common
QUERY_ENDPOINT = "https://api.cellphones.com.vn/v2/graphql/query"
PAGE_SIZE = 20
DEFAULT_TZ = "Asia/Ho_Chi_Minh"
BASE_HEADERS = {"Content-Type": "application/json"}

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
