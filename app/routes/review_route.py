from flask import Blueprint
from controllers.review_controller import review_controller
from middlewares.auth_middleware import user_required, auth_required

review_router = Blueprint("review_router", __name__)

# ==================== USER ROUTES ====================
# User tạo, sửa, xóa, xem reviews của mình

@review_router.route('/user', methods=['POST'])
@user_required
def create_review():
    """
    POST /api/reviews/user
    Body: { orderId, rating, comment }
    User tạo review cho đơn hàng
    """
    return review_controller.create()

@review_router.route('/user/<review_id>', methods=['PUT'])
@user_required
def update_review(review_id: str):
    """
    PUT /api/reviews/user/<review_id>
    Body: { rating?, comment? }
    User cập nhật review của mình
    """
    return review_controller.update(review_id)

@review_router.route('/user/<review_id>', methods=['DELETE'])
@user_required
def delete_review(review_id: str):
    """
    DELETE /api/reviews/user/<review_id>
    User xóa review của mình
    """
    return review_controller.delete(review_id)

@review_router.route('/user/my-reviews', methods=['GET'])
@user_required
def get_my_reviews():
    """
    GET /api/reviews/user/my-reviews
    User lấy danh sách reviews của mình
    """
    return review_controller.my_reviews()

@review_router.route('/order/<order_id>/check', methods=['GET'])
@user_required
def check_order_reviewable(order_id: str):
    """
    GET /api/reviews/order/<order_id>/check
    Kiểm tra đơn hàng có thể đánh giá không
    """
    return review_controller.check_reviewable(order_id)

@review_router.route('/user/reviewable-orders', methods=['GET'])
@user_required
def get_reviewable_orders():
    """
    GET /api/reviews/user/reviewable-orders
    Lấy danh sách đơn hàng Completed chưa được đánh giá (màn "Đánh giá ngay")
    """
    return review_controller.reviewable_orders()

# ==================== PUBLIC/SHARED ROUTES ====================
# Xem reviews (yêu cầu đăng nhập)

@review_router.route('/order/<order_id>', methods=['GET'])
@auth_required
def get_review_by_order(order_id: str):
    """
    GET /api/reviews/order/<order_id>
    Lấy review của 1 đơn hàng
    """
    return review_controller.get_by_order(order_id)

@review_router.route('/restaurant/<restaurant_id>', methods=['GET'])
@auth_required
def get_reviews_by_restaurant(restaurant_id: str):
    """
    GET /api/reviews/restaurant/<restaurant_id>
    Lấy tất cả reviews của nhà hàng (yêu cầu đăng nhập)
    """
    return review_controller.get_by_restaurant(restaurant_id)

@review_router.route('/restaurant/<restaurant_id>/stats', methods=['GET'])
@auth_required
def get_restaurant_rating_stats(restaurant_id: str):
    """
    GET /api/reviews/restaurant/<restaurant_id>/stats
    Lấy thống kê rating nhà hàng (yêu cầu đăng nhập)
    """
    return review_controller.get_restaurant_stats(restaurant_id)

@review_router.route('/food/<food_id>', methods=['GET'])
def get_reviews_by_food(food_id: str):
    """
    GET /api/reviews/food/<food_id>
    Lấy tất cả reviews của một món ăn (public - không cần auth)
    Format food_id: "restaurantId-foodName"
    """
    return review_controller.get_by_food_id(food_id)