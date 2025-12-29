from flask import Blueprint, request
from controllers.dashboard_controller import dashboard_controller
from middlewares.auth_middleware import admin_required, shipper_required

# Tạo Blueprint cho dashboard routes
dashboard_router = Blueprint('dashboard', __name__)


# ==================== Dashboard Routes ====================

@dashboard_router.route('/overview', methods=['GET'])
@admin_required
def get_overview():
    """
    GET /api/dashboard/overview
    Lấy thống kê tổng quan dashboard
    Yêu cầu: Admin
    """
    return dashboard_controller.get_overview()


@dashboard_router.route('/monthly-revenue', methods=['GET'])
@admin_required
def get_monthly_revenue():
    """
    GET /api/dashboard/monthly-revenue?year=2025
    Lấy biểu đồ doanh thu theo tháng
    Query params:
        - year: Năm cần xem (mặc định năm hiện tại)
    Yêu cầu: Admin
    """
    return dashboard_controller.get_monthly_revenue()


@dashboard_router.route('/order-status', methods=['GET'])
@admin_required
def get_order_status():
    """
    GET /api/dashboard/order-status
    Lấy phân bố trạng thái đơn hàng
    Yêu cầu: Admin
    """
    return dashboard_controller.get_order_status_distribution()


@dashboard_router.route('/recent-activities', methods=['GET'])
@admin_required
def get_recent_activities():
    """
    GET /api/dashboard/recent-activities?limit=10
    Lấy danh sách hoạt động gần đây
    Query params:
        - limit: Số lượng hoạt động muốn lấy (mặc định 10)
    Yêu cầu: Admin
    """
    return dashboard_controller.get_recent_activities()


@dashboard_router.route('/top-selling', methods=['GET'])
@admin_required
def get_top_selling():
    """
    GET /api/dashboard/top-selling?limit=10
    Lấy top món ăn bán chạy nhất
    Query params:
        - limit: Số lượng món muốn lấy (mặc định 10)
    Yêu cầu: Admin
    """
    return dashboard_controller.get_top_selling()


@dashboard_router.route('/top-restaurants', methods=['GET'])
@admin_required
def get_top_restaurants():
    """
    GET /api/dashboard/top-restaurants?limit=10
    Lấy top nhà hàng theo doanh thu
    Query params:
        - limit: Số lượng nhà hàng muốn lấy (mặc định 10)
    Yêu cầu: Admin
    """
    return dashboard_controller.get_top_restaurants()


@dashboard_router.route('/full', methods=['GET'])
@admin_required
def get_full_dashboard():
    """
    GET /api/dashboard/full
    Lấy tất cả dữ liệu dashboard cùng lúc (tối ưu performance)
    Yêu cầu: Admin
    """
    return dashboard_controller.get_full_dashboard()


# ==================== Shipper Dashboard Routes ====================

@dashboard_router.route('/shipper/overview', methods=['GET'])
@shipper_required
def get_shipper_overview():
    """
    GET /api/dashboard/shipper/overview
    Lấy thống kê tổng quan hôm nay cho shipper
    Yêu cầu: Shipper
    """
    return dashboard_controller.get_shipper_overview()


@dashboard_router.route('/shipper/activity', methods=['GET'])
@shipper_required
def get_shipper_activity():
    """
    GET /api/dashboard/shipper/activity
    Lấy lịch sử hoạt động của shipper
    Yêu cầu: Shipper
    """
    return dashboard_controller.get_shipper_activity()


@dashboard_router.route('/shipper/monthly-revenue', methods=['GET'])
@shipper_required
def get_shipper_monthly_revenue():
    """
    GET /api/dashboard/shipper/monthly-revenue?year=2025
    Lấy doanh thu theo tháng của shipper
    Query params:
        - year: Năm cần xem (mặc định năm hiện tại)
    Yêu cầu: Shipper
    """
    return dashboard_controller.get_shipper_monthly_revenue()


@dashboard_router.route('/shipper/full', methods=['GET'])
@shipper_required
def get_shipper_full_dashboard():
    """
    GET /api/dashboard/shipper/full
    Lấy tất cả dữ liệu dashboard cho shipper cùng lúc (tối ưu)
    Yêu cầu: Shipper
    """
    return dashboard_controller.get_shipper_full_dashboard()
