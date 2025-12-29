from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from .common import PyObjectId


class Review(BaseModel):
    """
    Model Review - Đánh giá của user cho nhà hàng sau khi hoàn thành đơn hàng
    
    Business Rules:
    - Mỗi order chỉ có thể review 1 lần (1-1 relationship)
    - Chỉ review được đơn đã Completed
    - Rating: 1-5 sao
    - User có thể edit review của mình
    """
    review_id: Optional[PyObjectId] = Field(default=None, alias="_id")
    order_id: PyObjectId = Field(..., alias="orderId", description="ID đơn hàng (unique)")
    user_id: PyObjectId = Field(..., alias="userId", description="ID người đánh giá")
    restaurant_id: PyObjectId = Field(..., alias="restaurantId", description="ID nhà hàng")
    
    # Thông tin review
    rating: int = Field(..., ge=1, le=5, description="Đánh giá sao (1-5)")
    comment: Optional[str] = Field(None, description="Nội dung đánh giá")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now, alias="createdAt")
    updated_at: datetime = Field(default_factory=datetime.now, alias="updatedAt")

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True

    @property
    def id(self):
        return self.review_id

    def to_dict(self):
        """Chuyển sang dict để trả về API"""
        return {
            "_id": str(self.review_id) if self.review_id else None,
            "orderId": str(self.order_id),
            "userId": str(self.user_id),
            "restaurantId": str(self.restaurant_id),
            "rating": self.rating,
            "comment": self.comment,
            "createdAt": self.created_at.isoformat(),
            "updatedAt": self.updated_at.isoformat(),
        }

    def to_mongo(self):
        """Chuyển sang dict để lưu MongoDB"""
        doc = {
            "orderId": self.order_id,
            "userId": self.user_id,
            "restaurantId": self.restaurant_id,
            "rating": self.rating,
            "comment": self.comment,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }
        if self.review_id:
            doc["_id"] = self.review_id
        return doc
