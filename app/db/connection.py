from pymongo import MongoClient
from core.config import config

# =========================
# MongoDB client & database
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
    """Trả về database instance"""
    return db


def get_collection(collection_name: str):
    """Lấy collection theo tên"""
    return db[collection_name]


def close_connection():
    """Đóng kết nối MongoDB"""
    client.close()


def ping_db():
    """Kiểm tra kết nối MongoDB"""
    try:
        client.admin.command('ping')
        return True
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        return False


def create_index_if_not_exists(collection, keys, **kwargs):
    """
    Tạo index chỉ khi chưa tồn tại
    keys: 'field' hoặc [('field', 1)]
    """
    existing_indexes = collection.index_information()

    # Chuẩn hoá keys về tuple để so sánh
    if isinstance(keys, str):
        keys_tuple = ((keys, 1),)
    else:
        keys_tuple = tuple(keys)

    for index in existing_indexes.values():
        if tuple(index.get('key', [])) == keys_tuple:
            return  # Index đã tồn tại → bỏ qua

    collection.create_index(keys, **kwargs)


# =========================
# Init indexes (SAFE)
# =========================

def init_indexes():
    """Tạo indexes an toàn (chỉ tạo nếu chưa tồn tại)"""
    try:
        # ===== USERS =====
        create_index_if_not_exists(users_collection, 'email', unique=True)

        # ===== RESTAURANTS =====
        create_index_if_not_exists(
            restaurants_collection,
            [('name', 1), ('address', 1)],
            unique=True
        )
        create_index_if_not_exists(restaurants_collection, [('name', 'text')])
        create_index_if_not_exists(restaurants_collection, 'menu.items.name')

        # ===== ORDERS =====
        create_index_if_not_exists(orders_collection, 'userId')
        create_index_if_not_exists(orders_collection, 'restaurantId')
        create_index_if_not_exists(orders_collection, 'shipperId')
        create_index_if_not_exists(orders_collection, 'status')
        create_index_if_not_exists(
            orders_collection,
            [('userId', 1), ('createdAt', -1)]
        )
        create_index_if_not_exists(
            orders_collection,
            [('restaurantId', 1), ('createdAt', -1)]
        )
        create_index_if_not_exists(
            orders_collection,
            [('shipperId', 1), ('createdAt', -1)]
        )
        create_index_if_not_exists(
            orders_collection,
            [('status', 1), ('createdAt', -1)]
        )

        # ===== PAYMENTS =====
        create_index_if_not_exists(payments_collection, 'orderId')
        create_index_if_not_exists(payments_collection, 'userId')
        create_index_if_not_exists(payments_collection, 'status')
        create_index_if_not_exists(
            payments_collection,
            [('userId', 1), ('createdAt', -1)]
        )

        # ===== VOUCHERS =====
        create_index_if_not_exists(vouchers_collection, 'code', unique=True)
        create_index_if_not_exists(vouchers_collection, 'active')
        create_index_if_not_exists(vouchers_collection, 'restaurantId')

        # ===== REVIEWS =====
        create_index_if_not_exists(reviews_collection, 'orderId', unique=True)
        create_index_if_not_exists(reviews_collection, 'userId')
        create_index_if_not_exists(reviews_collection, 'restaurantId')
        create_index_if_not_exists(
            reviews_collection,
            [('restaurantId', 1), ('createdAt', -1)]
        )
        create_index_if_not_exists(
            reviews_collection,
            [('userId', 1), ('createdAt', -1)]
        )

        # ===== CART (FIX DUPLICATE ERROR) =====
        create_index_if_not_exists(cart_collection, 'userId', unique=True)

        print("MongoDB indexes initialized safely ✅")

    except Exception as e:
        print(f"Error creating indexes: {e}")
