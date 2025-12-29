from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


# ============== REQUEST SCHEMAS ==============

class CreateReviewRequest(BaseModel):
    """Request tạo review mới"""
    order_id: str = Field(..., alias="orderId", description="ID đơn hàng")
    rating: int = Field(..., ge=1, le=5, description="Đánh giá 1-5 sao")
    comment: Optional[str] = Field(None, description="Nội dung đánh giá")

    class Config:
        populate_by_name = True


class UpdateReviewRequest(BaseModel):
    """Request cập nhật review"""
    rating: Optional[int] = Field(None, ge=1, le=5, description="Đánh giá 1-5 sao")
    comment: Optional[str] = Field(None, description="Nội dung đánh giá")

    class Config:
        populate_by_name = True


# ============== RESPONSE SCHEMAS ==============

class ReviewResponse(BaseModel):
    """Response review đầy đủ"""
    review_id: str = Field(..., alias="_id")
    order_id: str = Field(..., alias="orderId")
    user_id: str = Field(..., alias="userId")
    restaurant_id: str = Field(..., alias="restaurantId")
    rating: int
    comment: Optional[str] = None
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")
    
    # Thông tin bổ sung (sẽ hydrate từ service)
    user_fullname: Optional[str] = Field(None, alias="userFullname")
    restaurant_name: Optional[str] = Field(None, alias="restaurantName")

    class Config:
        populate_by_name = True


class RestaurantRatingStats(BaseModel):
    """Thống kê rating nhà hàng"""
    restaurant_id: str = Field(..., alias="restaurantId")
    average_rating: float = Field(..., alias="averageRating", description="Điểm trung bình")
    total_reviews: int = Field(..., alias="totalReviews", description="Tổng số đánh giá")
    rating_distribution: dict = Field(..., alias="ratingDistribution", description="Phân bố theo sao")

    class Config:
        populate_by_name = True
