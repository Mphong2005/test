from flask import Blueprint
from controllers.voucher_controller import voucher_controller
from middlewares.auth_middleware import admin_required, user_required

voucher_router = Blueprint("voucher_router", __name__)

# ==================== USER ROUTES ====================
# Voucher khả dụng cho user và preview discount

@voucher_router.route('/user/available', methods=['GET'])
@user_required
def get_available():
    """
    GET /api/vouchers/user/available?restaurantId=...
    Lấy danh sách voucher khả dụng cho user hiện tại
    """
    return voucher_controller.available()

@voucher_router.route('/user/expired', methods=['GET'])
@user_required
def get_expired():
    """
    GET /api/vouchers/user/expired?restaurantId=...
    Lấy danh sách voucher đã hết hạn cho user hiện tại
    """
    return voucher_controller.expired()

@voucher_router.route('/preview', methods=['POST'])
@user_required
def preview():
    """
    POST /api/vouchers/preview
    Body: { restaurantId, subtotal, shippingFee, promoId (hoặc code) }
    Preview số tiền giảm trước khi áp dụng voucher
    """
    return voucher_controller.preview()

# ==================== ADMIN ROUTES ====================
# CRUD voucher cho admin

@voucher_router.route('/admin', methods=['GET'])
@admin_required
def list_all():
    """
    GET /api/vouchers/admin
    Admin lấy danh sách tất cả voucher
    """
    return voucher_controller.list_all()

@voucher_router.route('/admin/available', methods=['GET'])
@admin_required
def admin_available():
    """
    GET /api/vouchers/admin/available?restaurantId=...
    Admin lấy danh sách voucher khả dụng (còn thể dùng)
    """
    return voucher_controller.available_for_admin()

@voucher_router.route('/admin/expired', methods=['GET'])
@admin_required
def list_expired():
    """
    GET /api/vouchers/admin/expired
    Admin lấy danh sách voucher đã hết hạn
    """
    return voucher_controller.list_expired()

@voucher_router.route('/admin', methods=['POST'])
@admin_required
def create():
    """
    POST /api/vouchers/admin
    Admin tạo voucher mới
    """
    return voucher_controller.create()

@voucher_router.route('/admin/<promo_id>', methods=['PUT'])
@admin_required
def update(promo_id: str):
    """
    PUT /api/vouchers/admin/<promo_id>
    Admin cập nhật voucher
    """
    return voucher_controller.update(promo_id)

@voucher_router.route('/admin/<promo_id>', methods=['DELETE'])
@admin_required
def delete(promo_id: str):
    """
    DELETE /api/vouchers/admin/<promo_id>
    Admin xóa voucher
    """
    return voucher_controller.delete(promo_id)
