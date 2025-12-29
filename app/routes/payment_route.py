from flask import Blueprint
from controllers.payment_controller import payment_controller
from middlewares.auth_middleware import admin_required, user_required

payment_router = Blueprint("payment_router", __name__)

# ==================== USER ROUTES ====================

@payment_router.route('/my', methods=['GET'])
@user_required
def get_my_payments():
    """GET /api/payments/my?status=Paid - Danh sách payment của tôi"""
    return payment_controller.get_my_payments()

@payment_router.route('/by_order/<order_id>', methods=['GET'])
@user_required
def get_payment_by_order(order_id: str):
    """GET /api/payments/by_order/<order_id> - Lấy payment theo order"""
    return payment_controller.get_by_order(order_id)

@payment_router.route('/<payment_id>', methods=['GET'])
@user_required
def get_payment(payment_id: str):
    """GET /api/payments/<payment_id> - Lấy chi tiết payment"""
    return payment_controller.get_payment(payment_id)

# ==================== ADMIN ROUTES ====================

@payment_router.route('/<payment_id>/mark_paid', methods=['PUT'])
@admin_required
def mark_paid(payment_id: str):
    """PUT /api/payments/<payment_id>/mark_paid - Admin xác nhận đã thanh toán"""
    return payment_controller.mark_paid(payment_id)

@payment_router.route('/<payment_id>/mark_failed', methods=['PUT'])
@admin_required
def mark_failed(payment_id: str):
    """PUT /api/payments/<payment_id>/mark_failed - Admin đánh dấu thất bại"""
    return payment_controller.mark_failed(payment_id)

@payment_router.route('/<payment_id>/refund', methods=['PUT'])
@admin_required
def refund(payment_id: str):
    """PUT /api/payments/<payment_id>/refund - Admin hoàn tiền"""
    return payment_controller.refund(payment_id)
