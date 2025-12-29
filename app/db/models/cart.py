from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from .common import PyObjectId


class CartItem(BaseModel):
    """Món ăn trong giỏ hàng"""
    restaurant_id: PyObjectId = Field(..., alias="restaurantId")
    restaurant_name: str = Field(..., alias="restaurantName")
    food_name: str = Field(..., alias="foodName")
    quantity: int = Field(..., ge=1)
    unit_price: float = Field(..., ge=0, alias="unitPrice")

    class Config:
        populate_by_name = True

    def to_dict(self):
        return {
            "restaurantId": str(self.restaurant_id),
            "restaurantName": self.restaurant_name,
            "foodName": self.food_name,
            "quantity": int(self.quantity),
            "unitPrice": float(self.unit_price)
        }


class Cart(BaseModel):
    """Giỏ hàng của user"""
    cart_id: Optional[PyObjectId] = Field(default=None, alias="_id")
    user_id: PyObjectId = Field(..., alias="userId")
    items: List[CartItem] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now, alias="createdAt")
    updated_at: datetime = Field(default_factory=datetime.now, alias="updatedAt")

    class Config:
        populate_by_name = True

    @property
    def id(self):
        return self.cart_id

    def to_dict(self):
        """Convert Cart object thành dictionary"""
        return {
            "_id": str(self.cart_id) if self.cart_id else None,
            "userId": str(self.user_id),
            "items": [item.to_dict() for item in self.items],
            "createdAt": self.created_at.isoformat(),
            "updatedAt": self.updated_at.isoformat()
        }

    def to_mongo(self):
        """Convert Cart object thành MongoDB document"""
        doc = {
            "userId": self.user_id,
            "items": [
                {
                    "restaurantId": item.restaurant_id,
                    "restaurantName": item.restaurant_name,
                    "foodName": item.food_name,
                    "quantity": item.quantity,
                    "unitPrice": item.unit_price
                }
                for item in self.items
            ],
            "createdAt": self.created_at,
            "updatedAt": self.updated_at
        }
        if self.cart_id:
            doc["_id"] = self.cart_id
        return doc
