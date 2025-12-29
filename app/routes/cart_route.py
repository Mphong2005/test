from flask import Blueprint
from controllers.cart_controller import cart_controller
from middlewares.auth_middleware import user_required

cart_router = Blueprint("cart_router", __name__)

# ==================== CART ROUTES ====================

@cart_router.route('', methods=['GET'])
@user_required
def get_cart():
    """GET /api/cart - Xem giỏ hàng"""
    return cart_controller.get_cart()

@cart_router.route('/items', methods=['POST'])
@user_required
def add_to_cart():
    """POST /api/cart/items - Thêm món vào giỏ"""
    return cart_controller.add_to_cart()

@cart_router.route('/items/<food_name>', methods=['PUT'])
@user_required
def update_cart_item(food_name: str):
    """PUT /api/cart/items/<food_name> - Cập nhật số lượng món"""
    return cart_controller.update_cart_item(food_name)

@cart_router.route('/items/<food_name>', methods=['DELETE'])
@user_required
def remove_from_cart(food_name: str):
    """DELETE /api/cart/items/<food_name> - Xóa món khỏi giỏ"""
    return cart_controller.remove_from_cart(food_name)

@cart_router.route('', methods=['DELETE'])
@user_required
def clear_cart():
    """DELETE /api/cart - Xóa toàn bộ giỏ hàng"""
    return cart_controller.clear_cart()

@cart_router.route('/checkout', methods=['POST'])
@user_required
def checkout():
    """POST /api/cart/checkout - Thanh toán giỏ hàng"""
    return cart_controller.checkout()
