from flask import Blueprint
from controllers.order_controller import order_controller
from middlewares.auth_middleware import admin_required, shipper_required, user_required

order_router = Blueprint("order_router", __name__)

# ==================== USER ROUTES ====================

@order_router.route('', methods=['POST'])
@user_required
def create_order():
    """POST /api/orders - Tạo đơn hàng"""
    return order_controller.create_order()

@order_router.route('/my_orders', methods=['GET'])
@user_required
def get_my_orders():
    """GET /api/orders/my_orders - Danh sách đơn hàng của user"""
    return order_controller.get_my_orders()

@order_router.route('/my_orders/filter', methods=['GET'])
@user_required
def filter_my_orders():
    """GET /api/orders/my_orders/filter?status=Pending - Lọc đơn hàng của user theo trạng thái"""
    return order_controller.filter_my_orders()

@order_router.route('/<order_id>', methods=['GET'])
@user_required
def get_order_detail(order_id: str):
    """GET /api/orders/<order_id> - Chi tiết đơn hàng"""
    return order_controller.get_order_detail(order_id)

@order_router.route('/<order_id>/cancel', methods=['PUT'])
@user_required
def user_cancel_order(order_id: str):
    """PUT /api/orders/<order_id>/cancel - User hủy đơn (PENDING only)"""
    return order_controller.user_cancel_order(order_id)

# ==================== SHIPPER ROUTES ====================

@order_router.route('/shipper/pending', methods=['GET'])
@shipper_required
def get_pending_orders():
    """GET /api/orders/shipper/pending - Danh sách đơn chờ (PENDING)"""
    return order_controller.get_pending_orders()

@order_router.route('/shipper/my_deliveries', methods=['GET'])
@shipper_required
def get_my_deliveries():
    """GET /api/orders/shipper/my_deliveries - Danh sách đơn của shipper"""
    return order_controller.get_my_deliveries()

@order_router.route('/<order_id>/accept', methods=['PUT'])
@shipper_required
def accept_order(order_id: str):
    """PUT /api/orders/<order_id>/accept - Shipper nhận đơn (PENDING → SHIPPING)"""
    return order_controller.accept_order(order_id)

@order_router.route('/<order_id>/completed', methods=['PUT'])
@shipper_required
def complete_delivery(order_id: str):
    """PUT /api/orders/<order_id>/completed - Shipper hoàn thành (SHIPPING → COMPLETED)"""
    return order_controller.complete_delivery(order_id)

@order_router.route('/<order_id>/decline', methods=['PUT'])
@shipper_required
def decline_order(order_id: str):
    """PUT /api/orders/<order_id>/decline - Shipper từ chối đơn PENDING (chưa nhận)"""
    return order_controller.decline_order(order_id)

@order_router.route('/<order_id>/reject', methods=['PUT'])
@shipper_required
def reject_order(order_id: str):
    """PUT /api/orders/<order_id>/reject - Shipper từ chối/hủy đơn đã nhận (SHIPPING → PENDING)"""
    return order_controller.reject_order(order_id)

@order_router.route('/shipper/<order_id>', methods=['GET'])
@shipper_required
def get_order_detail_for_shipper(order_id: str):
    """GET /api/orders/shipper/<order_id> - Shipper xem chi tiết đơn hàng"""
    return order_controller.get_order_detail_for_shipper(order_id)

@order_router.route('/shipper/filter', methods=['GET'])
@shipper_required
def get_deliveries_by_status():
    """GET /api/orders/shipper/filter?status=Shipping - Shipper lọc đơn theo trạng thái"""
    return order_controller.get_deliveries_by_status()

# ==================== ADMIN ROUTES ====================

@order_router.route('/all', methods=['GET'])
@admin_required
def get_all_orders():
    """GET /api/orders/all - Admin xem tất cả đơn hàng"""
    return order_controller.get_all_orders()

@order_router.route('/all/filter', methods=['GET'])
@admin_required
def filter_all_orders():
    """GET /api/orders/all/filter?status=Pending - Admin lọc tất cả đơn theo trạng thái"""
    return order_controller.filter_all_orders()

@order_router.route('/<order_id>/admin_cancel', methods=['PUT'])
@admin_required
def admin_cancel_order(order_id: str):
    """PUT /api/orders/<order_id>/admin_cancel - Admin hủy đơn (any status)"""
    return order_controller.admin_cancel_order(order_id)

@order_router.route('/restaurant/<restaurant_id>', methods=['GET'])
@admin_required
def get_restaurant_orders(restaurant_id: str):
    """GET /api/orders/restaurant/<restaurant_id> - Admin xem đơn hàng nhà hàng"""
    return order_controller.get_restaurant_orders(restaurant_id)
