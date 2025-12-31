from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import DuplicateKeyError
from core.config import config

# ======================
# MongoDB connection
# ======================
client = MongoClient(config.MONGO_URI)
db = client[config.MONGO_DB_NAME]

# Collections
users_collection = db['users']
restaurants_collection = db['restaurants']
orders_collection = db['orders']
payments_collection = db['payments']
vouchers_collection = db['vouchers']
reviews_collection = db['reviews']
cart_collection = db['cart']


# ======================
# Helpers
# ======================
def get_db():
    return db


def get_collection(collection_name: str):
    return db[collection_name]


def close_connection():
    client.close()


def ping_db():
    try:
        client.admin.command('ping')
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return False


def index_exists(collection, index_name: str) -> bool:
    """Kiểm tra index đã tồn tại chưa"""
    return index_name in collection.index_information()


def safe_create_index(collection, keys, **kwargs):
    """
    Tạo index an toàn:
    - Không tạo lại nếu đã tồn tại
    - Không làm crash app nếu bị DuplicateKey
    """
    try:
        index_name = kwargs.get("name")
        if index_name and index_exists(collection, index_name):
            return

        collection.create_index(keys, **kwargs)

    except DuplicateKeyError as e:
        print(f"⚠️  Skip index (duplicate data): {collection.name} | {keys}")
    except Exception as e:
        print(f"❌ Index error on {collection.name}: {e}")


# ======================
# Init indexes (SAFE)
# ======================
def init_indexes():
    try:
        # ===== USERS =====
        safe_create_index(
            users_collection,
            [('email', ASCENDING)],
            unique=True,
            name='users_email_unique'
        )

        # ===== RESTAURANTS =====
        safe_create_index(
            restaurants_collection,
            [('name', ASCENDING), ('address', ASCENDING)],
            unique=True,
            name='restaurants_name_address_unique'
        )

        safe_create_index(
            restaurants_collection,
            [('name', TEXT)],
            name='restaurants_name_text'
        )

        safe_create_index(
            restaurants_collection,
            [('menu.items.name', ASCENDING)],
            name='restaurants_menu_item_name'
        )

        # ===== ORDERS =====
        safe_create_index(orders_collection, [('userId', ASCENDING)], name='orders_userId')
        safe_create_index(orders_collection, [('restaurantId', ASCENDING)], name='orders_restaurantId')
        safe_create_index(orders_collection, [('shipperId', ASCENDING)], name='orders_shipperId')
        safe_create_index(orders_collection, [('status', ASCENDING)], name='orders_status')

        safe_create_index(
            orders_collection,
            [('userId', ASCENDING), ('createdAt', DESCENDING)],
            name='orders_user_createdAt'
        )

        safe_create_index(
            orders_collection,
            [('restaurantId', ASCENDING), ('createdAt', DESCENDING)],
            name='orders_restaurant_createdAt'
        )

        safe_create_index(
            orders_collection,
            [('shipperId', ASCENDING), ('createdAt', DESCENDING)],
            name='orders_shipper_createdAt'
        )

        safe_create_index(
            orders_collection,
            [('status', ASCENDING), ('createdAt', DESCENDING)],
            name='orders_status_createdAt'
        )

        # ===== PAYMENTS =====
        safe_create_index(payments_collection, [('orderId', ASCENDING)], name='payments_orderId')
        safe_create_index(payments_collection, [('userId', ASCENDING)], name='payments_userId')
        safe_create_index(payments_collection, [('status', ASCENDING)], name='payments_status')

        safe_create_index(
            payments_collection,
            [('userId', ASCENDING), ('createdAt', DESCENDING)],
            name='payments_user_createdAt'
        )

        # ===== VOUCHERS =====
        safe_create_index(
            vouchers_collection,
            [('code', ASCENDING)],
            unique=True,
            name='vouchers_code_unique'
        )

        safe_create_index(vouchers_collection, [('active', ASCENDING)], name='vouchers_active')
        safe_create_index(vouchers_collection, [('restaurantId', ASCENDING)], name='vouchers_restaurantId')

        # ===== REVIEWS =====
        safe_create_index(
            reviews_collection,
            [('orderId', ASCENDING)],
            unique=True,
            name='reviews_order_unique'
        )

        safe_create_index(reviews_collection, [('userId', ASCENDING)], name='reviews_userId')
        safe_create_index(reviews_collection, [('restaurantId', ASCENDING)], name='reviews_restaurantId')

        safe_create_index(
            reviews_collection,
            [('restaurantId', ASCENDING), ('createdAt', DESCENDING)],
            name='reviews_restaurant_createdAt'
        )

        safe_create_index(
            reviews_collection,
            [('userId', ASCENDING), ('createdAt', DESCENDING)],
            name='reviews_user_createdAt'
        )

        # ===== CART =====
        # ⚠️ Cart bị duplicate nên chỉ tạo nếu sạch dữ liệu
        safe_create_index(
            cart_collection,
            [('userId', ASCENDING)],
            unique=True,
            name='cart_user_unique'
        )

        print("✅ MongoDB indexes initialized safely")

    except Exception as e:
        print(f"❌ init_indexes failed: {e}")
