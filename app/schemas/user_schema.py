from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime
from db.models.user import GenderEnum, Role

class UserLoginRequest(BaseModel):
    """Schema cho đăng nhập"""
    email: EmailStr
    password: str

class UserRegisterRequest(BaseModel):
    """Schema cho đăng ký user"""
    fullname: str = Field(..., min_length=2, max_length=100, description="Tên người dùng")
    email: EmailStr = Field(..., description="Email")
    password: str = Field(..., min_length=6, description="Mật khẩu")
    phone_number: Optional[str] = Field(None, min_length=10, max_length=11, description="Số điện thoại")
    address: Optional[str] = Field(None, description="Địa chỉ")
    birthday: Optional[datetime] = Field(None, description="Ngày sinh")
    gender: Optional[GenderEnum] = Field(None, description="Giới tính")
    
    class Config:
        json_schema_extra = {
            "example": {
                "fullname": "John Doe",
                "email": "john@example.com",
                "password": "password123",
                "phone_number": "0909090909",
                "address": "123 Main St",
                "birthday": "1990-01-01",
                "gender": "Male"
            }
        }

class UserUpdateRequest(BaseModel):
    """Schema cho cập nhật thông tin user""" #form tương tự với form đăng kí
    fullname: Optional[str] = Field(None, min_length=2, max_length=100, description="Tên người dùng")
    email: Optional[EmailStr] = Field(None, description="Email")
    phone_number: Optional[str] = Field(None, min_length=10, max_length=11, description="Số điện thoại")
    address: Optional[str] = Field(None, description="Địa chỉ")
    birthday: Optional[datetime] = Field(None, description="Ngày sinh")
    gender: Optional[GenderEnum] = Field(None, description="Giới tính")

class UserResponse(BaseModel):
    """Schema cho response (không có password)""" #form in ra khi tìm kiếm
    user_id: str = Field(..., alias='_id')
    fullname: str
    email: EmailStr
    phone_number: Optional[str] = None
    address: Optional[str] = None
    balance: float
    birthday: Optional[datetime] = None    
    gender: Optional[GenderEnum] = None
    is_active: bool = Field(default=True, description="Trạng thái tài khoản (True: hoạt động, False: bị khóa)")
    created_at: datetime
    role: Role

class UserLoginResponse(BaseModel):
    """Schema cho response đăng nhập""" #form khi đăng nhập thành công, có thêm token
    user: UserResponse
    token: str  # Access token
    refresh_token: str  # Refresh token


class UserRoleUpdateRequest(BaseModel):
    """Schema cho cập nhật vai trò user (chỉ dành cho admin)"""
    role: Role


class UserTopUpRequest(BaseModel):
    """Schema cho nạp tiền vào tài khoản user"""
    amount: float = Field(..., gt=0, description="Số tiền nạp (> 0)")

    class Config:
        json_schema_extra = {"example": {"amount": 100000}}


class WithdrawRequest(BaseModel):
    """Schema cho shipper rút tiền từ balance"""
    amount: Optional[float] = Field(None, gt=0, description="Số tiền rút (> 0), nếu không truyền thì rút toàn bộ")

    class Config:
        json_schema_extra = {"example": {"amount": 500000}}


class RefreshTokenRequest(BaseModel):
    """Schema cho refresh token request"""
    refresh_token: str = Field(..., description="Refresh token để lấy access token mới")

    class Config:
        json_schema_extra = {"example": {"refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."}}


class RefreshTokenResponse(BaseModel):
    """Schema cho refresh token response"""
    token: str  # Access token mới