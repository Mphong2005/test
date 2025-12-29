from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict

from .common import PyObjectId


class PromotionType(str, Enum):
    """
    Enum định nghĩa các loại voucher:
    - PERCENT: Giảm theo phần trăm (vd: 20% off)
    - FIXED: Giảm số tiền cố định (vd: giảm 50k)
    - FREESHIP: Miễn phí ship (hoặc giảm phí ship với giới hạn)
    """
    PERCENT = "Percent"
    FIXED = "Fixed"
    FREESHIP = "Freeship"


class Promotion(BaseModel):
    """
    Model Voucher/Promotion
    Mục đích: Lưu thông tin voucher giảm giá cho đơn hàng
    
    Các trường quan trọng:
    - code: Mã voucher hiển thị cho user nhập
    - type: Loại voucher (Percent/Fixed/Freeship)
    - value: Giá trị giảm (% hoặc số tiền tùy type)
    - max_discount: Giảm tối đa (cho Percent và Freeship)
    - min_order_amount: Giá trị đơn hàng tối thiểu để áp dụng
    - restaurant_id: Nếu có thì voucher chỉ áp dụng cho nhà hàng đó, null = áp dụng toàn hệ thống
    - first_order_only: Voucher chỉ dành cho đơn đầu tiên của user
    - active: Trạng thái hoạt động
    - start_date/end_date: Thời gian hiệu lực
    """
    # Cho phép populate bằng tên field (snake_case) và chấp nhận các kiểu bson ObjectId
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    promo_id: Optional[PyObjectId] = Field(default=None, alias="_id")
    code: str = Field(..., description="Mã voucher hiển thị trên UI")
    promo_name: str
    type: PromotionType = PromotionType.PERCENT
    value: float = Field(..., description="Giá trị: % hoặc số tiền tùy theo type")
    max_discount: Optional[float] = Field(default=None, description="Giảm tối đa (cho Percent/Freeship)")
    min_order_amount: Optional[float] = None
    restaurant_id: Optional[PyObjectId] = Field(default=None, alias="restaurantId")
    first_order_only: bool = False
    active: bool = True
    start_date: datetime
    end_date: datetime
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now, alias="createdAt")

    @property
    def id(self):
        """Property để truy cập ID thuận tiện hơn"""
        return self.promo_id

    def to_dict(self):
        """Chuyển đổi sang dict để trả về JSON cho API"""
        return {
            "_id": str(self.promo_id) if self.promo_id else None,
            "code": self.code,
            "promo_name": self.promo_name,
            "type": self.type.value,
            "value": float(self.value),
            "max_discount": float(self.max_discount) if self.max_discount is not None else None,
            "min_order_amount": self.min_order_amount,
            "restaurantId": str(self.restaurant_id) if self.restaurant_id else None,
            "first_order_only": self.first_order_only,
            "active": self.active,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "description": self.description,
            "createdAt": self.created_at.isoformat(),
        }

    def to_mongo(self):
        """Chuyển đổi sang dict để lưu vào MongoDB"""
        doc = {
            "code": self.code,
            "promo_name": self.promo_name,
            "type": self.type.value,
            "value": float(self.value),
            "max_discount": float(self.max_discount) if self.max_discount is not None else None,
            "min_order_amount": self.min_order_amount,
            "restaurantId": self.restaurant_id,
            "first_order_only": self.first_order_only,
            "active": self.active,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "description": self.description,
            "createdAt": self.created_at,
        }
        if self.promo_id:
            doc["_id"] = self.promo_id
        return doc
