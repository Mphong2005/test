import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Config class chứa tất cả environment variables"""
    
    # MongoDB settings
    MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://23001508:23012005@cluster0.3g2rojb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
    MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'fooddelivery')
    
    #JWT 
    JWT_SECRET = os.getenv('JWT_SECRET')
    if JWT_SECRET:
        JWT_SECRET = JWT_SECRET.strip()  # Remove any whitespace
    JWT_ALGORITHM = os.getenv('JWT_ALGORITHM', 'HS256')
    ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', '1440'))  # default 1 day
    REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv('REFRESH_TOKEN_EXPIRE_DAYS', '7'))  # default 7 days
    
    # Validate required config
    if not JWT_SECRET:
        print("[WARNING] JWT_SECRET is not set in environment variables!")
        print("[WARNING] This will cause authentication to fail!")
    else:
        print(f"[INFO] JWT_SECRET is configured (length: {len(JWT_SECRET)})")

    # Flask settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'flask-secret-key-default')
    DEBUG = os.getenv('DEBUG', 'true').lower() == 'true'
    HOST = os.getenv('HOST', '127.0.0.1')
    PORT = int(os.getenv('PORT', '5000'))
    
config = Config()
