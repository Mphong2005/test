from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure
from core.config import config

# =========================
# MongoDB connection
# =========================

client = MongoClient(config.MONGO_URI)
db = client[config.MONGO_DB_NAME]

# =========================
# Collections
# =========================

users_collection = db['users']
restaurants_collection = db['restaurants']
orders_collection = db['orders']
payments_collection = db['payments']
vouchers_collection = db['vouchers']
reviews_collection = db['reviews']
cart_collection = db['cart']

# =========================
# Helpers
# =========================

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


# =========================
# Index utilities
# =========================

def index_with_keys_exists(collection, keys) -> bool:
    """
    Kiểm tra index đã tồn tại theo KEY (không quan tâm tên)
    """
    wanted = dict(keys)
    for index in collection.index_information().values():
        if dict(index.get('key', {})) == wanted:
            return True
    return False


def safe_create_index(collection, keys, **kwargs):
    """
    Tạo index an toàn:
    - Skip nếu index cùng KEY đã tồn tại
    - Skip nếu dữ liệu trùng (DuplicateKey)
    - Không throw lỗi làm crash app
    """
    try:
        if index_with_keys_exists(collection, keys):
            return

        collection.create_index(keys, **kwargs)

    except DuplicateKeyError:
        print(f"⚠️  Skip index (duplicate data): {collection.name} | {keys}")

    except OperationFailure as e:
        # IndexOptionsConflict → index đã tồn tại nhưng khác tên
        if e.code == 85:
            return
        print(f"❌ Index error on {collection.name}: {e}")

    except Exception as e:
        print(f"❌ Index error on {collection.name}: {e}")


# =========================
# Init indexes (RUN ON STARTUP)
# =========================

def init_indexes():
    """
    Tạo index an toàn cho toàn bộ collections
    """
    # ---------- USERS ----------
    safe_create_index(
        users_collection,
        [('email', 1)],
        unique=True
    )

    # ---------- RESTAURANTS ----------
    safe_create_index(
        restaurants_collection,
        [('name', 1), ('address', 1)],
        unique=True
    )

    safe_create_index(
        restaurants_collection,
        [('name', 'text')]
    )

    safe_create_index(
        restaurants_collection,
        [('menu.items.name', 1)]
    )

    # ---------- ORDERS ----------
    safe_create_index(orders_collection, [('userId', 1)])
    safe_create_index(orders_collection, [('restaurantId', 1)])
    safe_create_index(orders_collection, [('shipperId', 1)])
    safe_create_index(orders_collection, [('status', 1)])

    safe_create_index(
        orders_collection,
        [('userId', 1), ('createdAt', -1)]
    )

    safe_create_index(
        orders_collection,
        [('restaurantId', 1), ('createdAt', -1)]
    )

    safe_create_index(
        orders_collection,
        [('shipperId', 1), ('createdAt', -1)]
    )

    safe_create_index(
        orders_collection,
        [('status', 1), ('createdAt', -1)]
    )

    # ---------- PAYMENTS ----------
    safe_create_index(payments_collection, [('orderId', 1)])
    safe_create_index(payments_collection, [('userId', 1)])
    safe_create_index(payments_collection, [('status', 1)])

    safe_create_index(
        payments_collection,
        [('userId', 1), ('createdAt', -1)]
    )

    # ---------- VOUCHERS ----------
    safe_create_index(
        vouchers_collection,
        [('code', 1)],
        unique=True
    )

    safe_create_index(vouchers_collection, [('active', 1)])
    safe_create_index(vouchers_collection, [('restaurantId', 1)])

    # ---------- REVIEWS ----------
    safe_create_index(
        reviews_collection,
        [('orderId', 1)],
        unique=True
    )

    safe_create_index(reviews_collection, [('userId', 1)])
    safe_create_index(reviews_collection, [('restaurantId', 1)])

    safe_create_index(
        reviews_collection,
        [('restaurantId', 1), ('createdAt', -1)]
    )

    safe_create_index(
        reviews_collection,
        [('userId', 1), ('createdAt', -1)]
    )

    # ---------- CART ----------
    safe_create_index(
        cart_collection,
        [('userId', 1)],
        unique=True
    )

    print("✅ MongoDB indexes initialized safely")