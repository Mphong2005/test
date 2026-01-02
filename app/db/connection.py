from pymongo import MongoClient
from core.config import config

# Khởi tạo MongoDB client
client = MongoClient(config.MONGO_URI)

# Chọn database
db = client[config.MONGO_DB_NAME]

# Thêm các collections vào đây
users_collection = db['users']
restaurants_collection = db['restaurants']
orders_collection = db['orders']
payments_collection = db['payments']
vouchers_collection = db['vouchers']
reviews_collection = db['reviews']
cart_collection = db['cart']

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


# Tạo indexes (chạy 1 lần khi khởi động app)
def init_indexes():
    """Tạo indexes cho collections"""
    try:
        # Index cho users collection
        users_collection.create_index('email', unique=True)
        
        # Index cho restaurants collection
        # Unique index trên (name, address) combo - enforce logic trùng lặp
        restaurants_collection.create_index([('name', 1), ('address', 1)], unique=True)
        
        # Text index trên tên nhà hàng để hỗ trợ search theo tên
        restaurants_collection.create_index([('name', 'text')])
        
        # Index trên trường lồng nhau menu.items.name để hỗ trợ tìm món theo tên
        restaurants_collection.create_index('menu.items.name')
        
        # Index cho orders collection
        orders_collection.create_index('userId')
        orders_collection.create_index('restaurantId')
        orders_collection.create_index('shipperId')
        orders_collection.create_index('status')
        orders_collection.create_index([('userId', 1), ('createdAt', -1)])  # User orders sorted by date
        orders_collection.create_index([('restaurantId', 1), ('createdAt', -1)])  # Restaurant orders
        orders_collection.create_index([('shipperId', 1), ('createdAt', -1)])  # Shipper orders
        orders_collection.create_index([('status', 1), ('createdAt', -1)])  # Pending orders query

        # Index cho payments collection
        payments_collection.create_index('orderId')
        payments_collection.create_index('userId')
        payments_collection.create_index('status')
        payments_collection.create_index([('userId', 1), ('createdAt', -1)])

        # Index cho vouchers collection
        vouchers_collection.create_index('code', unique=True)
        vouchers_collection.create_index('active')
        vouchers_collection.create_index('restaurantId')
        
        # Index cho reviews collection
        reviews_collection.create_index('orderId', unique=True)  # 1 review per order
        reviews_collection.create_index('userId')
        reviews_collection.create_index('restaurantId')
        reviews_collection.create_index([('restaurantId', 1), ('createdAt', -1)])  # Restaurant reviews sorted
        reviews_collection.create_index([('userId', 1), ('createdAt', -1)])  # User reviews sorted
        
        # Index cho cart collection
        cart_collection.create_index('userId', unique=True)  # 1 cart per user
        
        print("MongoDB indexes created successfully")
    except Exception as e:
        print(f"Error creating indexes: {e}")