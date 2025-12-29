from flask import request, jsonify
from pydantic import ValidationError
from services.restaurant_service import restaurant_service
from schemas.restaurant_schema import (
    CreateRestaurantRequest,
    UpdateRestaurantRequest,
)

class RestaurantController:
    """Restaurant Controller - Xử lý HTTP requests cho nhà hàng"""

    def list_all(self):
        """User xem danh sách nhà hàng đang hoạt động"""
        try:
            data = restaurant_service.get_all_restaurants()
            return jsonify({'success': True, 'data': data}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def admin_list_all(self):
        """Admin xem TẤT CẢ nhà hàng (bao gồm cả bị khóa)"""
        try:
            data = restaurant_service.admin_get_all_restaurants()
            return jsonify({'success': True, 'data': data}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_by_id(self, restaurant_id: str):
        """User xem chi tiết nhà hàng (chỉ nếu đang hoạt động)"""
        try:
            data = restaurant_service.get_restaurant_by_id(restaurant_id)
            return jsonify({'success': True, 'data': data}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 404
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def admin_get_by_id(self, restaurant_id: str):
        """Admin xem chi tiết nhà hàng (kể cả bị khóa)"""
        try:
            data = restaurant_service.admin_get_restaurant_by_id(restaurant_id)
            return jsonify({'success': True, 'data': data}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 404
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def create(self):
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            req = CreateRestaurantRequest(**request.json)
            result = restaurant_service.create_restaurant(req)
            return jsonify({'success': True, 'message': 'Tạo nhà hàng thành công', 'data': result}), 201
        except ValidationError as e:
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def update(self, restaurant_id: str):
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            req = UpdateRestaurantRequest(**request.json)
            result = restaurant_service.update_restaurant(restaurant_id, req)
            return jsonify({'success': True, 'message': 'Cập nhật nhà hàng thành công', 'data': result}), 200
        except ValidationError as e:
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def delete(self, restaurant_id: str):
        try:
            result = restaurant_service.delete_restaurant(restaurant_id)
            return jsonify({'success': True, 'message': result['message']}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def search_for_users(self):
        """Search cho user: chỉ tìm food và category"""
        try:
            q = request.args.get('q')
            if not q:
                return jsonify({'success': False, 'message': 'Thiếu tham số q'}), 400
            data = restaurant_service.search_for_users(q)
            return jsonify({'success': True, 'data': data}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def search_for_admin(self):
        """Search cho admin: tìm restaurant, food và category"""
        try:
            q = request.args.get('q')
            if not q:
                return jsonify({'success': False, 'message': 'Thiếu tham số q'}), 400
            data = restaurant_service.search_for_admin(q)
            return jsonify({'success': True, 'data': data}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def toggle_status(self, restaurant_id: str):
        """Admin kích hoạt/vô hiệu hóa nhà hàng"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            
            status = request.json.get('status')
            if status is None:
                return jsonify({'success': False, 'message': 'Thiếu trường status (true/false)'}), 400
            
            result = restaurant_service.toggle_restaurant_status(restaurant_id, status)
            return jsonify({'success': True, 'message': result['message'], 'data': result['restaurant']}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_promotions(self):
        """Lấy danh sách promotions từ restaurants có reviews tốt nhất"""
        try:
            limit = request.args.get('limit', 8, type=int)
            data = restaurant_service.get_promotions(limit=limit)
            return jsonify({'success': True, 'data': data}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_categories(self):
        """Lấy danh sách categories từ restaurants với hình ảnh random"""
        try:
            data = restaurant_service.get_categories()
            return jsonify({'success': True, 'data': data}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_all_foods(self):
        """Lấy tất cả foods từ restaurants đang hoạt động"""
        try:
            data = restaurant_service.get_all_foods()
            return jsonify({'success': True, 'data': data}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_food_by_id(self, food_id: str):
        """Lấy food item theo ID"""
        try:
            data = restaurant_service.get_food_by_id(food_id)
            if data:
                return jsonify({'success': True, 'data': data}), 200
            else:
                return jsonify({'success': False, 'message': 'Không tìm thấy món ăn'}), 404
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

restaurant_controller = RestaurantController()
