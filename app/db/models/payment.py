from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict

from .common import PyObjectId


class PaymentStatus(str, Enum):
    PENDING = "Pending"
    PAID = "Paid"
    FAILED = "Failed"
    REFUNDED = "Refunded"


class PaymentMethod(str, Enum):
    COD = "COD"
    BALANCE = "Balance"


class Payment(BaseModel):
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    payment_id: Optional[PyObjectId] = Field(default=None, alias="_id")
    order_id: PyObjectId = Field(alias="orderId")
    user_id: PyObjectId = Field(alias="userId")
    amount: float
    method: PaymentMethod = PaymentMethod.COD
    status: PaymentStatus = PaymentStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.now, alias="createdAt")
    updated_at: datetime = Field(default_factory=datetime.now, alias="updatedAt")

    @property
    def id(self):
        return self.payment_id

    def to_dict(self):
        return {
            "_id": str(self.payment_id) if self.payment_id else None,
            "orderId": str(self.order_id),
            "userId": str(self.user_id),
            "amount": float(self.amount),
            "method": self.method.value,
            "status": self.status.value,
            "createdAt": self.created_at.isoformat(),
            "updatedAt": self.updated_at.isoformat(),
        }

    def to_mongo(self):
        doc = {
            "orderId": self.order_id,
            "userId": self.user_id,
            "amount": float(self.amount),
            "method": self.method.value,
            "status": self.status.value,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }
        if self.payment_id:
            doc["_id"] = self.payment_id
        return doc


