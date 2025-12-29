from typing import Optional, List
from pydantic import BaseModel, Field
from .common import PyObjectId

# 1. Class Món ăn (Cấp thấp nhất)
class FoodMenuItem(BaseModel):
    name: str 
    price: float
    description: Optional[str] = None
    image: Optional[str] = None
    status: Optional[bool] = True
    
    class Config:
        populate_by_name = True

    def to_dict(self):
        return {
            "name": self.name,
            "price": self.price,
            "description": self.description,
            "image": self.image,
            "status": self.status
        }

# 2. Class Category (Cấp giữa - chứa danh sách món)
class MenuCategory(BaseModel):
    category: str 
    items: List[FoodMenuItem] = Field(default_factory=list)

    class Config:
        populate_by_name = True

    def to_dict(self):
        return {
            "category": self.category,
            "items": [item.to_dict() for item in self.items]
        }

# 3. Class Restaurant (Cấp cao nhất - chứa menu)
class Restaurant(BaseModel):
    restaurant_id: Optional[PyObjectId] = Field(default=None, alias="_id")
    restaurant_name: str = Field(..., alias="name") 
    email: Optional[str] = None
    address: Optional[str] = None
    hotline: Optional[str] = None
    open_time: Optional[str] = Field(default=None, alias="openTime")
    close_time: Optional[str] = Field(default=None, alias="closeTime")
    map_link: Optional[str] = Field(default=None, alias="mapLink")
    status: bool = Field(default=True, description="Trạng thái hoạt động của nhà hàng")
    
    # Review statistics
    average_rating: float = Field(default=0.0, alias="averageRating", description="Điểm đánh giá trung bình")
    total_reviews: int = Field(default=0, alias="totalReviews", description="Tổng số đánh giá")
    
    # Menu là list các Category
    menu: Optional[List[MenuCategory]] = Field(default_factory=list)

    class Config:
        populate_by_name = True
    
    @property
    def id(self):
        return self.restaurant_id

    def to_dict(self):
        """Dùng để trả về JSON cho API"""
        return {
            "_id": str(self.restaurant_id) if self.restaurant_id else None,
            "name": self.restaurant_name,
            "email": self.email,
            "address": self.address,
            "hotline": self.hotline,
            "openTime": self.open_time,
            "closeTime": self.close_time,
            "mapLink": self.map_link,
            "status": self.status,
            "averageRating": float(self.average_rating),
            "totalReviews": int(self.total_reviews),
            "menu": [cat.to_dict() for cat in self.menu] if self.menu else []
        }

    def to_mongo(self):
        """Dùng để lưu vào MongoDB"""
        doc = {
            "name": self.restaurant_name,
            "email": self.email,
            "address": self.address,
            "hotline": self.hotline,
            "openTime": self.open_time,
            "closeTime": self.close_time,
            "mapLink": self.map_link,
            "status": self.status,
            "average_rating": float(self.average_rating),
            "total_reviews": int(self.total_reviews),
            # Lưu menu dưới dạng dict lồng nhau
            "menu": [cat.to_dict() for cat in self.menu] if self.menu else []
        }
        if self.restaurant_id:
            doc["_id"] = self.restaurant_id
        return doc