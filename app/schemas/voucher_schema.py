from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime
from db.models.vouchers import PromotionType


# ============== REQUEST SCHEMAS ==============

class CreatePromotionRequest(BaseModel):
    """
    Request tạo voucher mới
    - code: Mã voucher (unique)
    - promo_name: Tên hiển thị
    - type: Loại voucher (Percent/Fixed/Freeship)
    - value: Giá trị giảm (% hoặc số tiền)
    - max_discount: Giảm tối đa (optional, cho Percent và Freeship)
    - min_order_amount: Giá trị đơn tối thiểu (optional)
    - restaurant_id: ID nhà hàng (optional, null = áp dụng toàn hệ thống)
    - first_order_only: Chỉ áp dụng cho đơn đầu tiên
    - active: Trạng thái hoạt động
    - start_date/end_date: Thời gian hiệu lực
    """
    code: str = Field(..., description="Mã voucher")
    promo_name: str = Field(..., description="Tên voucher")
    type: PromotionType = PromotionType.PERCENT
    value: float = Field(..., gt=0, description="Giá trị")
    max_discount: Optional[float] = Field(None, ge=0, description="Giảm tối đa")
    min_order_amount: Optional[float] = Field(None, ge=0)
    restaurant_id: Optional[str] = Field(None, alias="restaurantId")
    first_order_only: bool = False
    active: bool = True
    start_date: datetime
    end_date: datetime
    description: Optional[str] = None

    class Config:
        populate_by_name = True


class UpdatePromotionRequest(BaseModel):
    """
    Request cập nhật voucher
    Tất cả fields đều optional - chỉ cập nhật những field được truyền lên
    """
    promo_name: Optional[str] = None
    type: Optional[PromotionType] = None
    value: Optional[float] = Field(None, ge=0)
    max_discount: Optional[float] = Field(None, ge=0)
    min_order_amount: Optional[float] = Field(None, ge=0)
    restaurant_id: Optional[str] = Field(None, alias="restaurantId")
    first_order_only: Optional[bool] = None
    active: Optional[bool] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    description: Optional[str] = None

    class Config:
        populate_by_name = True


# ============== RESPONSE SCHEMAS ==============

class PromotionResponse(BaseModel):
    """
    Response voucher đầy đủ
    Dùng khi trả về thông tin chi tiết voucher cho client
    """
    promo_id: str = Field(..., alias="_id")
    code: str
    promo_name: str
    type: PromotionType
    value: float
    max_discount: Optional[float] = None
    min_order_amount: Optional[float] = None
    restaurant_id: Optional[str] = Field(None, alias="restaurantId")
    first_order_only: bool
    active: bool
    start_date: datetime
    end_date: datetime
    description: Optional[str] = None
    usage_count: int = 0
    user_usage_history: list = Field(default_factory=list)
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")

    class Config:
        populate_by_name = True
