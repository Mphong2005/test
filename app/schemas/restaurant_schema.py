from pydantic import BaseModel, Field
from typing import Optional, List
from db.models.restaurants import FoodMenuItem, MenuCategory


class AddFoodToMenuRequest(BaseModel):
    """
    Request thêm món. 
    Không có food_id.
    Backend sẽ dùng 'category' để tìm nhóm và append vào danh sách items.
    """
    name: str
    price: float
    description: Optional[str] = None
    image: Optional[str] = None
    status: Optional[bool] = True
    category: str = Field(...) 

    class Config:
        populate_by_name = True


class UpdateFoodInMenuRequest(BaseModel):
    """
    Schema chứa các thông tin CẦN SỬA.
    Lưu ý: 
    - Định danh món ăn (Tên cũ) sẽ nằm trên URL API.
    - Field 'name' ở đây là TÊN MỚI (dùng khi muốn đổi tên món).
    - Field 'category' ở đây là CATEGORY MỚI (dùng khi muốn chuyển nhóm).
    """
    name: Optional[str] = None
    price: Optional[float] = None
    description: Optional[str] = None
    image: Optional[str] = None
    status: Optional[bool] = None
    category: Optional[str] = None # Dùng nếu muốn chuyển món sang category khác

    class Config:
        populate_by_name = True


class CreateRestaurantRequest(BaseModel):
    restaurant_name: str = Field(..., alias="name")
    email: Optional[str] = None
    address: Optional[str] = None
    hotline: Optional[str] = None
    open_time: Optional[str] = Field(default=None, alias="openTime")
    close_time: Optional[str] = Field(default=None, alias="closeTime")
    map_link: Optional[str] = Field(default=None, alias="mapLink")
    status: Optional[bool] = Field(default=True, description="Trạng thái hoạt động")
    
    # Input menu lồng nhau: [{category: "Pizza", items: [...]}, ...]
    menu: Optional[List[MenuCategory]] = Field(default_factory=list)

    class Config:
        populate_by_name = True


class UpdateRestaurantRequest(BaseModel):
    restaurant_name: Optional[str] = Field(None, alias="name")
    email: Optional[str] = None
    address: Optional[str] = None
    hotline: Optional[str] = None
    open_time: Optional[str] = Field(default=None, alias="openTime")
    close_time: Optional[str] = Field(default=None, alias="closeTime")
    map_link: Optional[str] = Field(default=None, alias="mapLink")
    status: Optional[bool] = None

    class Config:
        populate_by_name = True


# ============== RESPONSE SCHEMAS ==============

class RestaurantSimpleResponse(BaseModel):
    """
    Response rút gọn: Chỉ chứa thông tin cơ bản, KHÔNG CÓ MENU.
    Dùng để nhúng vào kết quả tìm kiếm cho nhẹ.
    """
    restaurant_id: str = Field(..., alias="_id") 
    restaurant_name: str = Field(..., alias="name")
    email: Optional[str] = None
    address: Optional[str] = None
    hotline: Optional[str] = None
    open_time: Optional[str] = Field(default=None, alias="openTime")
    close_time: Optional[str] = Field(default=None, alias="closeTime")
    map_link: Optional[str] = Field(default=None, alias="mapLink")
    status: bool = True
    average_rating: float = Field(default=0.0, alias="averageRating")
    total_reviews: int = Field(default=0, alias="totalReviews")

    class Config:
        populate_by_name = True


class RestaurantResponse(RestaurantSimpleResponse):
    """
    Response đầy đủ: Kế thừa từ bản rút gọn và thêm MENU.
    Dùng khi lấy chi tiết 1 nhà hàng.
    """
    menu: Optional[List[MenuCategory]] = Field(default_factory=list)


class SearchFoodResponse(BaseModel):
    """
    Response tìm kiếm: Trả về món ăn + thông tin nhà hàng (Rút gọn).
    """
    food: FoodMenuItem
    category: str # Trả thêm tên category để biết món nằm ở đâu
    
    # SỬA: Dùng bản rút gọn để ẩn menu
    restaurant: RestaurantSimpleResponse

    class Config:
        populate_by_name = True