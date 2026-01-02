from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field

from .common import PyObjectId
from .payment import PaymentMethod
from utils.timezone_utils import get_utc_now


class OrderItemStatus(str, Enum):
    ACTIVE = "Active"
    CANCELLED = "Cancelled"


class OrderItem(BaseModel):
    """Mục đích: Lưu thông tin từng món ăn trong đơn hàng"""
    food_name: str
    quantity: int = Field(..., ge=1)
    unit_price: float = Field(..., ge=0)
    subtotal: float = Field(...)  # quantity * unit_price
    status: OrderItemStatus = OrderItemStatus.ACTIVE

    class Config:
        populate_by_name = True

    def to_dict(self):
        return {
            "food_name": self.food_name,
            "quantity": int(self.quantity),
            "unit_price": float(self.unit_price),
            "subtotal": float(self.subtotal),
            "status": self.status.value
        }


class OrderStatus(str, Enum):
    PENDING = "Pending"
    SHIPPING = "Shipping"
    COMPLETED = "Completed"
    CANCELLED = "Cancelled"


class ShipperRejectionEntry(BaseModel):
    """Lần từ chối của shipper cho một đơn hàng"""
    shipper_id: PyObjectId = Field(alias="shipperId")
    reason: Optional[str] = None
    timestamp: datetime

    class Config:
        populate_by_name = True

    def to_dict(self):
        return {
            "shipperId": str(self.shipper_id),
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat(),
        }


class Order(BaseModel):
    """Mục đích: Lưu thông tin đơn hàng với items embedded"""
    
    # === Thông tin cơ bản (ID) ===
    order_id: Optional[PyObjectId] = Field(default=None, alias="_id")
    user_id: PyObjectId = Field(alias="userId")
    restaurant_id: PyObjectId = Field(alias="restaurantId")
    shipper_id: Optional[PyObjectId] = Field(default=None, alias="shipperId")
    payment_id: Optional[PyObjectId] = Field(default=None, alias="paymentId")
    payment_method: Optional[PaymentMethod] = Field(default=None, alias="paymentMethod")
    
    # === Denormalized: Thông tin User (cho Shipper xem) ===
    user_fullname: str = Field(alias="userFullname")  # Tên khách hàng
    user_phone: str = Field(alias="userPhone")  # SĐT khách hàng
    
    # === Denormalized: Thông tin Restaurant (cho Shipper lấy hàng) ===
    restaurant_name: str = Field(alias="restaurantName")  # Tên quán
    restaurant_address: str = Field(alias="restaurantAddress")  # Địa chỉ quán
    restaurant_hotline: Optional[str] = Field(default=None, alias="restaurantHotline")  # Hotline quán
    
    # === Chi tiết đơn ===
    items: List[OrderItem] = Field(default_factory=list)
    address: str
    note: Optional[str] = None
    
    # === Tiền ===
    subtotal: float = Field(..., ge=0)
    shipping_fee: float = Field(default=0, ge=0)
    discount: float = Field(default=0, ge=0)
    total_amount: float = Field(..., ge=0)
    
    # === Voucher ===
    promo_id: Optional[PyObjectId] = Field(default=None, alias="promoId")
    
    # === Trạng thái ===
    status: OrderStatus = OrderStatus.PENDING
    is_reviewed: bool = Field(default=False, alias="isReviewed")  # Đã đánh giá chưa
    refunded: bool = False
    refunded_amount: float = 0.0
    refund_at: Optional[datetime] = None
    # === Lịch sử từ chối của shipper ===
    shipper_rejections: List[ShipperRejectionEntry] = Field(default_factory=list, alias="shipperRejections")
    
    # === Hủy đơn ===
    cancelled_by: Optional[str] = None  # "user" / "admin"
    cancellation_reason: Optional[str] = None
    
    # === Thời gian ===
    created_at: datetime = Field(default_factory=get_utc_now, alias="createdAt")
    updated_at: datetime = Field(default_factory=get_utc_now, alias="updatedAt")
    picked_at: Optional[datetime] = Field(default=None, alias="pickedAt", description="Thời điểm shipper nhận đơn")

    class Config:
        populate_by_name = True

    @property
    def id(self):
        return self.order_id

    def to_dict(self):
        """Dùng để trả về JSON cho API"""
        return {
            "_id": str(self.order_id) if self.order_id else None,
            "userId": str(self.user_id),
            "restaurantId": str(self.restaurant_id),
            "shipperId": str(self.shipper_id) if self.shipper_id else None,
            "userFullname": self.user_fullname,
            "userPhone": self.user_phone,
            "restaurantName": self.restaurant_name,
            "restaurantAddress": self.restaurant_address,
            "restaurantHotline": self.restaurant_hotline,
            "items": [item.to_dict() for item in self.items],
            "address": self.address,
            "note": self.note,
            "subtotal": float(self.subtotal),
            "shipping_fee": float(self.shipping_fee),
            "discount": float(self.discount),
            "total_amount": float(self.total_amount),
            "promoId": str(self.promo_id) if self.promo_id else None,
            "paymentId": str(self.payment_id) if self.payment_id else None,
            "paymentMethod": self.payment_method.value if self.payment_method else None,
            "status": self.status.value,
            "isReviewed": self.is_reviewed,
            "refunded": self.refunded,
            "refunded_amount": float(self.refunded_amount),
            "refund_at": self.refund_at.isoformat() if self.refund_at else None,
            "cancelled_by": self.cancelled_by,
            "cancellation_reason": self.cancellation_reason,
            "shipperRejections": [entry.to_dict() for entry in self.shipper_rejections],
            "createdAt": self.created_at.isoformat(),
            "updatedAt": self.updated_at.isoformat(),
            "pickedAt": self.picked_at.isoformat() if self.picked_at else None,
        }

    def to_mongo(self):
        """Dùng để lưu vào MongoDB"""
        doc = {
            "userId": self.user_id,
            "restaurantId": self.restaurant_id,
            "shipperId": self.shipper_id,
            "userFullname": self.user_fullname,
            "userPhone": self.user_phone,
            "restaurantName": self.restaurant_name,
            "restaurantAddress": self.restaurant_address,
            "restaurantHotline": self.restaurant_hotline,
            "items": [item.to_dict() for item in self.items],
            "address": self.address,
            "note": self.note,
            "subtotal": float(self.subtotal),
            "shipping_fee": float(self.shipping_fee),
            "discount": float(self.discount),
            "total_amount": float(self.total_amount),
            "promoId": self.promo_id,
            "paymentId": self.payment_id,
            "paymentMethod": self.payment_method.value if self.payment_method else None,
            "status": self.status.value,
            "isReviewed": self.is_reviewed,
            "refunded": self.refunded,
            "refunded_amount": float(self.refunded_amount),
            "refund_at": self.refund_at,
            "cancelled_by": self.cancelled_by,
            "cancellation_reason": self.cancellation_reason,
            "shipperRejections": [
                {
                    "shipperId": entry.shipper_id,
                    "reason": entry.reason,
                    "timestamp": entry.timestamp,
                } for entry in self.shipper_rejections
            ],
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "pickedAt": self.picked_at,
        }
        if self.order_id:
            doc["_id"] = self.order_id
        return doc
