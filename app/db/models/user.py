from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime
from enum import Enum
from utils.roles import Role
from .common import PyObjectId

class GenderEnum(str, Enum):
    MALE = "Male"
    FEMALE = "Female"

class User(BaseModel):
    """Các thuộc tính của users"""
    user_id: Optional[PyObjectId] = Field(default=None, alias='_id')
    fullname: Optional[str] = None
    email: Optional[EmailStr] = None
    password: str  
    phone_number: Optional[str] = None
    address: Optional[str] = None
    balance: float = 0.0
    birthday: Optional[datetime] = None
    gender: Optional[GenderEnum] = None
    is_active: bool = Field(default=True, description="Trạng thái tài khoản (True: hoạt động, False: bị khóa)")
    created_at: datetime = Field(default_factory=datetime.now)
    role: Role

    @property
    def id(self):
        """Alias cho user_id để dễ sử dụng"""
        return self.user_id

    def to_dict(self):
        """Convert User object thành dictionary"""
        return {
            '_id': str(self.user_id) if self.user_id else None,
            'fullname': self.fullname,
            'email': self.email,
            'phone_number': self.phone_number,
            'address': self.address,
            'balance': float(self.balance),
            'birthday': self.birthday.isoformat() if self.birthday else None,
            'gender': self.gender.value if self.gender else None,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat(),
            'role': self.role.value if self.role else None
        }

    def to_mongo(self):
        """Convert User object thành MongoDB document"""
        doc = {
            'fullname': self.fullname,
            'email': self.email,
            'password': self.password,
            'phone_number': self.phone_number,
            'address': self.address,
            'balance': float(self.balance),
            'birthday': self.birthday,
            'gender': self.gender.value if self.gender else None,
            'is_active': self.is_active,
            'created_at': self.created_at,
            'role': self.role.value if self.role else None
        }
        if self.user_id:
            doc['_id'] = self.user_id
        return doc