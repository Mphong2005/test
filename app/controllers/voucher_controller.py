from flask import request, jsonify
from pydantic import ValidationError
from pymongo.errors import DuplicateKeyError

from services.voucher_service import voucher_service
from schemas.voucher_schema import (
    CreatePromotionRequest,
    UpdatePromotionRequest,
)


class VoucherController:
    """
    Voucher Controller - Xử lý HTTP requests cho voucher/promotion
    
    Phân quyền:
    - User: available (danh sách voucher khả dụng), preview (xem trước discount)
    - Admin: CRUD (create, update, delete, list_all)
    """

    # ===== Admin CRUD =====
    def create(self):
        """Admin tạo voucher mới"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            req = CreatePromotionRequest(**request.json)
            # Dump theo field name (snake_case), schema đã map alias -> field
            data = req.model_dump()
            result = voucher_service.create(data)
            return jsonify({'success': True, 'message': 'Tạo voucher thành công', 'data': result}), 201
        except ValidationError as e:
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except DuplicateKeyError:
            return jsonify({'success': False, 'message': 'Mã voucher đã tồn tại. Vui lòng chọn mã khác.'}), 409
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception:
            return jsonify({'success': False, 'message': 'Lỗi máy chủ. Vui lòng thử lại sau.'}), 500

    def update(self, promo_id: str):
        """Admin cập nhật voucher"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            req = UpdatePromotionRequest(**request.json)
            data = req.model_dump()
            result = voucher_service.update(promo_id, data)
            return jsonify({'success': True, 'message': 'Cập nhật voucher thành công', 'data': result}), 200
        except ValidationError as e:
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def delete(self, promo_id: str):
        """Admin xóa voucher"""
        try:
            voucher_service.delete(promo_id)
            return jsonify({'success': True, 'message': 'Xóa voucher thành công'}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def list_all(self):
        """Admin lấy danh sách tất cả voucher"""
        try:
            result = voucher_service.list_all()
            return jsonify({'success': True, 'data': result}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def list_expired(self):
        """Admin lấy danh sách voucher đã hết hạn"""
        try:
            result = voucher_service.get_expired_for_user(None, None)  # Admin xem hết
            return jsonify({'success': True, 'data': result}), 200
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def available_for_admin(self):
        """
        Admin lấy danh sách voucher khả dụng (còn thể dùng)
        Query params: ?restaurantId=... (optional)
        """
        try:
            restaurant_id = request.args.get('restaurantId')
            result = voucher_service.get_all_available(restaurant_id)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    # ===== User facing =====
    def available(self):
        """
        User lấy danh sách voucher khả dụng
        Query params: ?restaurantId=... (optional)
        """
        try:
            user_id = getattr(request, 'user_id', None)
            if not user_id:
                return jsonify({'success': False, 'message': 'Không tìm thấy thông tin user'}), 401
            
            restaurant_id = request.args.get('restaurantId')
            print(f"voucher_controller.available: user_id={user_id}, restaurant_id={restaurant_id}")
            result = voucher_service.get_available_for_user(user_id, restaurant_id)
            print(f"voucher_controller.available: Returning {len(result)} vouchers")
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            print(f"voucher_controller.available: ValueError: {e}")
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            print(f"voucher_controller.available: Exception: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def expired(self):
        """
        User lấy danh sách voucher đã hết hạn
        Query params: ?restaurantId=... (optional)
        """
        try:
            user_id = getattr(request, 'user_id', None)
            if not user_id:
                return jsonify({'success': False, 'message': 'Không tìm thấy thông tin user'}), 401
            
            restaurant_id = request.args.get('restaurantId')
            print(f"voucher_controller.expired: user_id={user_id}, restaurant_id={restaurant_id}")
            result = voucher_service.get_expired_for_user(user_id, restaurant_id)
            print(f"voucher_controller.expired: Returning {len(result)} vouchers")
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            print(f"voucher_controller.expired: ValueError: {e}")
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            print(f"voucher_controller.expired: Exception: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def preview(self):
        """
        User preview discount trước khi đặt hàng
        Body: { restaurantId, subtotal, shippingFee, promoId (hoặc code) }
        """
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            user_id = request.user_id
            restaurant_id = request.json.get('restaurantId')
            subtotal = float(request.json.get('subtotal', 0))
            shipping_fee = float(request.json.get('shippingFee', 0))
            promo_id = request.json.get('promoId')
            code = request.json.get('code')
            if not restaurant_id:
                return jsonify({'success': False, 'message': 'Thiếu restaurantId'}), 400
            result = voucher_service.preview_discount(user_id, restaurant_id, subtotal, shipping_fee, promo_id, code)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500


voucher_controller = VoucherController()
