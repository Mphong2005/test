from pydantic import BaseModel, Field
from typing import Optional, List


# ============== REQUEST SCHEMAS ==============

class AddToCartRequest(BaseModel):
    """Request thêm món vào giỏ"""
    restaurant_id: str = Field(..., alias="restaurantId")
    food_name: str = Field(..., alias="foodName")
    quantity: int = Field(..., ge=1)
    
    class Config:
        populate_by_name = True


class UpdateCartItemRequest(BaseModel):
    """Request cập nhật số lượng món trong giỏ"""
    quantity: int = Field(..., ge=0, description="Số lượng mới, 0 để xóa")
    
    class Config:
        populate_by_name = True


class CheckoutRequest(BaseModel):
    """Request thanh toán giỏ hàng"""
    address: str = Field(..., description="Địa chỉ giao hàng")
    note: Optional[str] = Field(None, description="Ghi chú cho đơn hàng")
    payment_method: str = Field(..., alias="paymentMethod", description="Phương thức thanh toán")
    shipping_fee: float = Field(default=0, ge=0, alias="shippingFee")
    promo_id: Optional[str] = Field(None, alias="promoId")
    
    class Config:
        populate_by_name = True


# ============== RESPONSE SCHEMAS ==============

class CartItemResponse(BaseModel):
    """Response item trong giỏ"""
    restaurant_id: str = Field(..., alias="restaurantId")
    restaurant_name: str = Field(..., alias="restaurantName")
    food_name: str = Field(..., alias="foodName")
    quantity: int
    unit_price: float = Field(..., alias="unitPrice")
    subtotal: float
    
    class Config:
        populate_by_name = True


class CartResponse(BaseModel):
    """Response giỏ hàng đầy đủ"""
    cart_id: str = Field(..., alias="_id")
    user_id: str = Field(..., alias="userId")
    items: List[CartItemResponse]
    total_items: int = Field(..., alias="totalItems")
    total_amount: float = Field(..., alias="totalAmount")
    created_at: str = Field(..., alias="createdAt")
    updated_at: str = Field(..., alias="updatedAt")
    
    class Config:
        populate_by_name = True
