from pydantic import BaseModel, Field, model_serializer
from typing import Optional, List, Any
from datetime import datetime
from db.models.order import OrderItem, OrderStatus
from db.models.payment import PaymentMethod


# ============== REQUEST SCHEMAS ==============

class CreateOrderItemRequest(BaseModel):
    """Request item trong đơn - CHỈ LẤY TÊN VÀ SỐ LƯỢNG, GIÁ LẤY TỬ DB"""
    food_name: str = Field(..., description="Tên món ăn")
    quantity: int = Field(..., ge=1, description="Số lượng")

    class Config:
        populate_by_name = True


class CreateOrderRequest(BaseModel):
    """Request tạo đơn hàng - Backend tự tính discount từ promoId, người dùng không thể truyền"""
    restaurant_id: str = Field(..., alias="restaurantId", description="ID nhà hàng")
    items: List[CreateOrderItemRequest] = Field(..., description="Danh sách món")
    address: str = Field(..., description="Địa chỉ giao")
    note: Optional[str] = Field(None, description="Ghi chú")
    payment_method: PaymentMethod = Field(..., description="Phương thức thanh toán")
    shipping_fee: float = Field(default=0, ge=0, description="Phí ship")
    promo_id: Optional[str] = Field(None, alias="promoId", description="ID voucher")

    class Config:
        populate_by_name = True


class UpdateOrderStatusRequest(BaseModel):
    """Request cập nhật trạng thái đơn"""
    status: OrderStatus = Field(..., description="Trạng thái mới")

    class Config:
        populate_by_name = True


class CancelOrderRequest(BaseModel):
    """Request hủy đơn"""
    reason: Optional[str] = Field(None, description="Lý do hủy")

    class Config:
        populate_by_name = True


class AssignShipperRequest(BaseModel):
    """Request gán shipper cho đơn (chỉ admin/system)"""
    shipper_id: str = Field(..., alias="shipperId", description="ID shipper")

    class Config:
        populate_by_name = True


# ============== RESPONSE SCHEMAS ==============

class OrderItemResponse(BaseModel):
    """Response item"""
    food_name: str
    quantity: int
    unit_price: float
    subtotal: float

    class Config:
        populate_by_name = True


class ShipperRejectionResponse(BaseModel):
    """Lịch sử từ chối của shipper cho đơn hàng"""
    shipper_id: str = Field(..., alias="shipperId")
    reason: Optional[str] = None
    timestamp: datetime

    class Config:
        populate_by_name = True


class OrderResponse(BaseModel):
    """Response đơn hàng đầy đủ"""
    order_id: str = Field(..., alias="_id")
    user_id: str = Field(..., alias="userId")
    restaurant_id: str = Field(..., alias="restaurantId")
    shipper_id: Optional[str] = Field(None, alias="shipperId")
    # Thông tin shipper bổ sung trong chi tiết đơn
    shipper: Optional[dict] = None
    user_fullname: str = Field(..., alias="userFullname")
    user_phone: str = Field(..., alias="userPhone")
    restaurant_name: str = Field(..., alias="restaurantName")
    restaurant_address: str = Field(..., alias="restaurantAddress")
    restaurant_hotline: Optional[str] = Field(None, alias="restaurantHotline")
    items: List[OrderItemResponse]
    address: str
    note: Optional[str] = None
    subtotal: float
    shipping_fee: float
    discount: float
    total_amount: float
    promo_id: Optional[str] = Field(None, alias="promoId")
    payment_id: Optional[str] = Field(None, alias="paymentId")
    payment_method: Optional[PaymentMethod] = Field(None, alias="paymentMethod")
    status: str
    refunded: bool = False
    refunded_amount: float = 0
    refund_at: Optional[datetime] = None
    cancelled_by: Optional[str] = None
    cancellation_reason: Optional[str] = None
    shipper_rejections: List[ShipperRejectionResponse] = Field(default_factory=list, alias="shipperRejections")
    picked_at: Optional[datetime] = Field(None, alias="pickedAt")
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")

    @model_serializer
    def serialize_model(self) -> dict[str, Any]:
        """Custom serializer: Loại bỏ shipperId khi đã có object shipper"""
        data = {
            "_id": self.order_id,
            "userId": self.user_id,
            "restaurantId": self.restaurant_id,
            "userFullname": self.user_fullname,
            "userPhone": self.user_phone,
            "restaurantName": self.restaurant_name,
            "restaurantAddress": self.restaurant_address,
            "restaurantHotline": self.restaurant_hotline,
            "items": self.items,
            "address": self.address,
            "note": self.note,
            "subtotal": self.subtotal,
            "shipping_fee": self.shipping_fee,
            "discount": self.discount,
            "total_amount": self.total_amount,
            "promoId": self.promo_id,
            "paymentId": self.payment_id,
            "paymentMethod": self.payment_method,
            "status": self.status,
            "refunded": self.refunded,
            "refunded_amount": self.refunded_amount,
            "refund_at": self.refund_at,
            "cancelled_by": self.cancelled_by,
            "cancellation_reason": self.cancellation_reason,
            "shipperRejections": self.shipper_rejections,
            "pickedAt": self.picked_at,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }
        
        # Chỉ thêm shipperId khi KHÔNG có object shipper
        if self.shipper:
            data["shipper"] = self.shipper
        else:
            data["shipperId"] = self.shipper_id
        
        return data

    class Config:
        populate_by_name = True


class OrderSimpleResponse(BaseModel):
    """Response rút gọn (danh sách)"""
    order_id: str = Field(..., alias="_id")
    user_id: str = Field(..., alias="userId")
    restaurant_id: str = Field(..., alias="restaurantId")
    user_fullname: str = Field(..., alias="userFullname")
    user_phone: str = Field(..., alias="userPhone")
    user_email: Optional[str] = Field(None, alias="userEmail")  # Email của user (bổ sung từ user service)
    restaurant_name: str = Field(..., alias="restaurantName")
    restaurant_address: str = Field(..., alias="restaurantAddress")
    address: str
    items: List[OrderItemResponse] = Field(default_factory=list)
    food_name: Optional[str] = Field(None, alias="foodName")  # Tên món đầu tiên để hiển thị
    shipping_fee: float
    total_amount: float
    status: str
    is_reviewed: bool = Field(default=False, alias="isReviewed")
    imageUrl: Optional[str] = Field(None, alias="imageUrl")
    refunded: bool = False
    refunded_amount: float = 0
    refund_at: Optional[datetime] = None
    created_at: datetime = Field(..., alias="createdAt")
    # Thông tin shipper bổ sung (nếu có)
    shipper: Optional[dict] = None

    class Config:
        populate_by_name = True
