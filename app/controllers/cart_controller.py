from flask import request, jsonify
from pydantic import ValidationError
from services.cart_service import cart_service
from schemas.cart_schema import (
    AddToCartRequest,
    UpdateCartItemRequest,
    CheckoutRequest
)


class CartController:
    """Cart Controller - Xử lý HTTP requests cho giỏ hàng"""

    def get_cart(self):
        """Lấy giỏ hàng của user"""
        try:
            user_id = request.user_id
            result = cart_service.get_cart(user_id)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def add_to_cart(self):
        """Thêm món vào giỏ"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            
            req = AddToCartRequest(**request.json)
            user_id = request.user_id
            result = cart_service.add_to_cart(user_id, req)
            return jsonify({'success': True, 'message': 'Đã thêm vào giỏ hàng', 'data': result}), 200
        except ValidationError as e:
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def update_cart_item(self, food_name: str):
        """Cập nhật số lượng món trong giỏ"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            
            req = UpdateCartItemRequest(**request.json)
            user_id = request.user_id
            result = cart_service.update_cart_item(user_id, food_name, req)
            return jsonify({'success': True, 'message': 'Đã cập nhật giỏ hàng', 'data': result}), 200
        except ValidationError as e:
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def remove_from_cart(self, food_name: str):
        """Xóa món khỏi giỏ"""
        try:
            user_id = request.user_id
            result = cart_service.remove_from_cart(user_id, food_name)
            return jsonify({'success': True, 'message': 'Đã xóa món khỏi giỏ hàng', 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def clear_cart(self):
        """Xóa toàn bộ giỏ hàng"""
        try:
            user_id = request.user_id
            result = cart_service.clear_cart(user_id)
            return jsonify({'success': True, 'message': 'Đã xóa toàn bộ giỏ hàng', 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def checkout(self):
        """Thanh toán giỏ hàng"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            
            req = CheckoutRequest(**request.json)
            user_id = request.user_id
            result = cart_service.checkout_cart(user_id, req)
            return jsonify({'success': True, 'message': 'Đặt hàng thành công', 'data': result}), 201
        except ValidationError as e:
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500


cart_controller = CartController()
