from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class OverviewStats(BaseModel):
    """Thống kê tổng quan dashboard"""
    total_revenue_month: float = Field(..., alias="totalRevenueMonth", description="Tổng doanh thu tháng hiện tại")
    revenue_today: float = Field(..., alias="revenueToday", description="Doanh thu hôm nay")
    active_users: int = Field(..., alias="activeUsers", description="Số người dùng active")
    total_restaurants: int = Field(..., alias="totalRestaurants", description="Tổng số nhà hàng")

    class Config:
        populate_by_name = True


class MonthlyRevenueData(BaseModel):
    """Doanh thu theo tháng"""
    month: str = Field(..., description="Tháng (T1, T2, ...)")
    revenue: float = Field(..., description="Doanh thu của tháng đó")

    class Config:
        populate_by_name = True


class MonthlyRevenueResponse(BaseModel):
    """Response biểu đồ doanh thu theo tháng"""
    year: int = Field(..., description="Năm")
    data: List[MonthlyRevenueData] = Field(..., description="Dữ liệu doanh thu từng tháng")

    class Config:
        populate_by_name = True


class OrderStatusDistribution(BaseModel):
    """Phân bố trạng thái đơn hàng"""
    completed: int = Field(..., description="Số đơn hoàn thành")
    pending: int = Field(..., description="Số đơn đang chờ xử lý")
    shipping: int = Field(..., description="Số đơn đang giao")
    cancelled: int = Field(..., description="Số đơn đã hủy")

    class Config:
        populate_by_name = True


class RecentActivity(BaseModel):
    """Hoạt động gần đây"""
    order_id: str = Field(..., alias="orderId")
    user_name: str = Field(..., alias="userName")
    status: str = Field(..., description="Trạng thái đơn hàng")
    timestamp: str = Field(..., description="Thời gian hoạt động")
    type: str = Field(..., description="Loại hoạt động: đặt đơn, giao thành công, v.v.")

    class Config:
        populate_by_name = True


class RecentActivitiesResponse(BaseModel):
    """Response danh sách hoạt động gần đây"""
    activities: List[RecentActivity]

    class Config:
        populate_by_name = True


class TopSellingItem(BaseModel):
    """Món ăn bán chạy nhất"""
    food_name: str = Field(..., alias="foodName")
    restaurant_name: str = Field(..., alias="restaurantName", description="Tên nhà hàng")
    restaurant_id: str = Field(..., alias="restaurantId", description="ID nhà hàng")
    total_sold: int = Field(..., alias="totalSold", description="Số lượng đã bán")

    class Config:
        populate_by_name = True


class TopSellingResponse(BaseModel):
    """Response top món bán chạy"""
    items: List[TopSellingItem]

    class Config:
        populate_by_name = True


class TopRevenueRestaurant(BaseModel):
    """Nhà hàng có doanh thu cao nhất"""
    restaurant_id: str = Field(..., alias="restaurantId")
    restaurant_name: str = Field(..., alias="restaurantName")
    total_revenue: float = Field(..., alias="totalRevenue", description="Tổng doanh thu")

    class Config:
        populate_by_name = True


class TopRevenueRestaurantsResponse(BaseModel):
    """Response top nhà hàng theo doanh thu"""
    restaurants: List[TopRevenueRestaurant]

    class Config:
        populate_by_name = True


class DashboardFullResponse(BaseModel):
    """Response đầy đủ cho dashboard (tất cả dữ liệu cùng lúc)"""
    overview: OverviewStats
    monthly_revenue: MonthlyRevenueResponse = Field(..., alias="monthlyRevenue")
    order_status: OrderStatusDistribution = Field(..., alias="orderStatus")
    recent_activities: List[RecentActivity] = Field(..., alias="recentActivities")
    top_selling: List[TopSellingItem] = Field(..., alias="topSelling")
    top_restaurants: List[TopRevenueRestaurant] = Field(..., alias="topRestaurants")

    class Config:
        populate_by_name = True


# ==================== Shipper Dashboard Schemas ====================

class ShipperOverviewStats(BaseModel):
    """Thống kê tổng quan cho shipper"""
    today_income: float = Field(..., alias="todayIncome", description="Phi nhập hôm nay")
    today_completed_orders: int = Field(..., alias="todayCompletedOrders", description="Số đơn giao thành công hôm nay")
    today_hours: float = Field(..., alias="todayHours", description="Giờ hoạt động hôm nay")

    class Config:
        populate_by_name = True


class ShipperActivityHistory(BaseModel):
    """Lịch sử hoạt động shipper"""
    month_orders: int = Field(..., alias="monthOrders", description="Tổng đơn tháng này")
    accumulated_income: float = Field(..., alias="accumulatedIncome", description="Thu nhập tích lũy")
    total_hours: float = Field(..., alias="totalHours", description="Tổng thời gian làm")

    class Config:
        populate_by_name = True


class ShipperMonthlyRevenueData(BaseModel):
    """Doanh thu theo tháng của shipper"""
    month: str = Field(..., description="Tháng (T1, T2, ...)")
    orders: int = Field(..., description="Số đơn hoàn thành")
    revenue: float = Field(..., description="Doanh thu (shipping_fee)")

    class Config:
        populate_by_name = True


class ShipperDashboardFullResponse(BaseModel):
    """Response đầy đủ cho shipper dashboard"""
    overview: ShipperOverviewStats
    activity_history: ShipperActivityHistory = Field(..., alias="activityHistory")
    monthly_revenue: List[ShipperMonthlyRevenueData] = Field(..., alias="monthlyRevenue")

    class Config:
        populate_by_name = True
