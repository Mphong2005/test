from flask import Blueprint
from controllers.restaurant_controller import restaurant_controller
from middlewares.auth_middleware import admin_required, user_required, auth_required, user_or_admin_required

restaurant_router = Blueprint("restaurant_router", __name__)

# ==================== USER ROUTES ====================
# Lấy danh sách nhà hàng đang hoạt động
@restaurant_router.route('/all', methods=['GET'])
@auth_required
def list_all():
    """User xem danh sách nhà hàng đang hoạt động"""
    return restaurant_controller.list_all()

# Lấy danh sách promotions (public - không cần auth)
@restaurant_router.route('/promotions', methods=['GET'])
def get_promotions():
    """Lấy danh sách promotions từ restaurants có reviews tốt nhất"""
    return restaurant_controller.get_promotions()

# Lấy danh sách categories (public - không cần auth)
@restaurant_router.route('/categories', methods=['GET'])
def get_categories():
    """Lấy danh sách categories từ restaurants với hình ảnh random"""
    return restaurant_controller.get_categories()

# Lấy tất cả foods (public - không cần auth)
@restaurant_router.route('/foods', methods=['GET'])
def get_all_foods():
    """Lấy tất cả foods từ restaurants đang hoạt động"""
    return restaurant_controller.get_all_foods()

# Lấy food by id (public - không cần auth)
@restaurant_router.route('/foods/<food_id>', methods=['GET'])
def get_food_by_id(food_id: str):
    """Lấy food item theo ID"""
    return restaurant_controller.get_food_by_id(food_id)

# Lấy chi tiết nhà hàng (public - không cần auth, chỉ nhà hàng đang hoạt động)
@restaurant_router.route('/<restaurant_id>', methods=['GET'])
def get_by_id_public(restaurant_id: str):
    """Public xem chi tiết nhà hàng đang hoạt động (không cần đăng nhập)"""
    return restaurant_controller.get_by_id(restaurant_id)

# Lấy chi tiết nhà hàng (yêu cầu auth - cho user đã đăng nhập)
@restaurant_router.route('/user/<restaurant_id>', methods=['GET'])
@user_or_admin_required
def get_by_id(restaurant_id: str):
    """User xem chi tiết nhà hàng đang hoạt động (yêu cầu đăng nhập)"""
    return restaurant_controller.get_by_id(restaurant_id)

# Search cho user: chỉ tìm food và category ở quán đang hoạt động
@restaurant_router.route('/search', methods=['GET'])
@user_required
def search_for_users():
    return restaurant_controller.search_for_users()

# ==================== ADMIN ROUTES ====================
# Admin lấy TẤT CẢ nhà hàng (bao gồm cả bị khóa)
@restaurant_router.route('/admin/all', methods=['GET'])
@admin_required
def admin_list_all():
    """Admin xem TẤT CẢ nhà hàng"""
    return restaurant_controller.admin_list_all()

# Admin xem chi tiết nhà hàng (kể cả bị khóa)
@restaurant_router.route('/admin/<restaurant_id>', methods=['GET'])
@admin_required
def admin_get_by_id(restaurant_id: str):
    """Admin xem chi tiết nhà hàng (kể cả bị khóa)"""
    return restaurant_controller.admin_get_by_id(restaurant_id)

# Search cho admin: tìm restaurant, food và category
@restaurant_router.route('/search_admin', methods=['GET'])
@admin_required
def search_for_admin():
    return restaurant_controller.search_for_admin()

# Tạo nhà hàng mới - Chỉ admin
@restaurant_router.route('', methods=['POST'])
@admin_required
def create():
    return restaurant_controller.create()

# Cập nhật nhà hàng - Chỉ admin
@restaurant_router.route('/<restaurant_id>', methods=['PUT'])
@admin_required
def update(restaurant_id: str):
    return restaurant_controller.update(restaurant_id)

# Xóa nhà hàng - Chỉ admin
@restaurant_router.route('/<restaurant_id>', methods=['DELETE'])
@admin_required
def delete(restaurant_id: str):
    return restaurant_controller.delete(restaurant_id)

# Kích hoạt/Vô hiệu hóa nhà hàng - Chỉ admin
@restaurant_router.route('/<restaurant_id>/toggle-status', methods=['PUT'])
@admin_required
def toggle_status(restaurant_id: str):
    """
    PUT /api/restaurants/<restaurant_id>/toggle-status - Admin kích hoạt/vô hiệu hóa nhà hàng
    Body: {"status": true/false}
    """
    return restaurant_controller.toggle_status(restaurant_id)
