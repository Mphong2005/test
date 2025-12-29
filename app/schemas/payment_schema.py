from pydantic import BaseModel, Field
from datetime import datetime
from db.models.payment import PaymentStatus, PaymentMethod


class PaymentResponse(BaseModel):
    """Schema trả về thông tin payment"""
    payment_id: str = Field(..., alias="_id")
    order_id: str = Field(..., alias="orderId")
    user_id: str = Field(..., alias="userId")
    amount: float
    method: PaymentMethod
    status: PaymentStatus
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")

    class Config:
        populate_by_name = True
