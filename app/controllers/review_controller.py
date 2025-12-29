from flask import request, jsonify
from pydantic import ValidationError

from services.review_service import review_service
from schemas.review_schema import CreateReviewRequest, UpdateReviewRequest


class ReviewController:
    """
    Review Controller - Xử lý HTTP requests cho review
    
    Phân quyền:
    - User: create, update, delete own reviews, list own reviews, check reviewable
    - Admin: list all reviews by restaurant
    """

    # ===== User endpoints =====
    
    def create(self):
        """User tạo review cho đơn hàng"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            
            print(f"[ReviewController.create] Request body: {request.json}")
            user_id = request.user_id
            print(f"[ReviewController.create] user_id: {user_id}")
            
            req = CreateReviewRequest(**request.json)
            print(f"[ReviewController.create] Parsed request - order_id: {req.order_id}, rating: {req.rating}")
            
            result = review_service.create(
                order_id=req.order_id,
                user_id=user_id,
                rating=req.rating,
                comment=req.comment
            )
            
            return jsonify({'success': True, 'message': 'Đánh giá thành công', 'data': result}), 201
        except ValidationError as e:
            print(f"[ReviewController.create] ValidationError: {e.errors()}")
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            print(f"[ReviewController.create] ValueError: {str(e)}")
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            print(f"[ReviewController.create] Exception: {str(e)}")
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def update(self, review_id: str):
        """User cập nhật review của mình"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            
            user_id = request.user_id
            req = UpdateReviewRequest(**request.json)
            
            result = review_service.update(
                review_id=review_id,
                user_id=user_id,
                rating=req.rating,
                comment=req.comment
            )
            
            return jsonify({'success': True, 'message': 'Cập nhật đánh giá thành công', 'data': result}), 200
        except ValidationError as e:
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def delete(self, review_id: str):
        """User xóa review của mình"""
        try:
            user_id = request.user_id
            review_service.delete(review_id, user_id)
            return jsonify({'success': True, 'message': 'Xóa đánh giá thành công'}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def my_reviews(self):
        """User lấy danh sách reviews của mình"""
        try:
            user_id = request.user_id
            reviews = review_service.find_by_user_id(user_id)
            return jsonify({'success': True, 'data': reviews}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_by_order(self, order_id: str):
        """User/Admin lấy review của 1 đơn hàng"""
        try:
            review = review_service.find_by_order_id(order_id)
            if not review:
                return jsonify({'success': False, 'message': 'Đơn hàng chưa có đánh giá'}), 404
            return jsonify({'success': True, 'data': review_service._to_dict(review)}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def check_reviewable(self, order_id: str):
        """User kiểm tra đơn hàng có thể đánh giá không"""
        try:
            user_id = request.user_id
            result = review_service.check_order_reviewable(order_id, user_id)
            return jsonify({'success': True, 'data': result}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def reviewable_orders(self):
        """User lấy danh sách đơn Completed chưa review"""
        try:
            user_id = request.user_id
            data = review_service.get_reviewable_orders(user_id)
            return jsonify({'success': True, 'data': data}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    # ===== Admin/Public endpoints =====
    
    def get_by_restaurant(self, restaurant_id: str):
        """Lấy tất cả reviews của nhà hàng (public/admin)"""
        try:
            reviews = review_service.find_by_restaurant_id(restaurant_id)
            return jsonify({'success': True, 'data': reviews}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_restaurant_stats(self, restaurant_id: str):
        """Lấy thống kê rating nhà hàng (public/admin)"""
        try:
            stats = review_service.get_restaurant_rating_stats(restaurant_id)
            return jsonify({'success': True, 'data': stats}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_by_food_id(self, food_id: str):
        """Lấy tất cả reviews của một món ăn (public)"""
        try:
            reviews = review_service.find_by_food_id(food_id)
            return jsonify({'success': True, 'data': reviews}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500


review_controller = ReviewController()
