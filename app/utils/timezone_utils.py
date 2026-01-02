"""
Timezone Utilities
Các hàm tiện ích để xử lý thời gian thống nhất với UTC

CÁCH SỬ DỤNG ĐÚNG:
- Backend: Luôn dùng get_utc_now() để lưu thời gian UTC
- Frontend: Convert UTC sang Vietnam time (+7 giờ) khi hiển thị
"""

from datetime import datetime, timezone, timedelta


def get_utc_now() -> datetime:
    """
    Lấy thời gian hiện tại dạng UTC với timezone info.
    Sử dụng hàm này thay cho datetime.now() trong tất cả services.
    
    Returns:
        datetime: Thời gian UTC hiện tại với tzinfo=UTC
    """
    return datetime.now(timezone.utc)


# Alias cho backward compatibility - nhưng khuyến khích dùng get_utc_now()
def get_vietnam_now() -> datetime:
    """
    DEPRECATED: Không nên dùng vì MongoDB sẽ convert sang UTC anyway.
    Dùng get_utc_now() thay thế.
    
    Trả về giờ UTC (để MongoDB không convert thêm lần nữa)
    """
    return get_utc_now()


def format_datetime_iso(dt: datetime) -> str:
    """
    Format datetime thành ISO string với timezone info.
    
    Args:
        dt: datetime object
        
    Returns:
        str: ISO formatted string (e.g., "2026-01-02T10:06:20+00:00")
    """
    if dt is None:
        return ""
    return dt.isoformat()


def to_utc(dt: datetime) -> datetime:
    """
    Convert datetime sang UTC.
    Nếu datetime không có timezone info, giả định là UTC.
    
    Args:
        dt: datetime object
        
    Returns:
        datetime: datetime ở UTC
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Naive datetime - giả định là UTC
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
