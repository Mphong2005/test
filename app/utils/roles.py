from enum import Enum

class Role(str, Enum):
    """Tất cả vai trò trong hệ thống"""
    ADMIN = "admin"
    USER = "user"
    SHIPPER = "shipper"
    
class LimitedRole(str, Enum): 
    """Vai trò có thể chọn khi đăng ký (không bao gồm Admin)"""
    USER = "user" 
    SHIPPER = "shipper"
