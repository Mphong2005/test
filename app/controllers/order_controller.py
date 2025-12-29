from flask import request, jsonify
from pydantic import ValidationError
from services.order_service import order_service
from schemas.order_schema import (
    CreateOrderRequest,
    CancelOrderRequest,
)


class OrderController:
    """Order Controller - Xử lý HTTP requests cho đơn hàng"""

    # ==================== User Routes ====================

    def create_order(self):
        """Tạo đơn hàng (User)"""
        try:
            if not request.json:
                return jsonify({'success': False, 'message': 'Request body không được để trống'}), 400
            
            print(f"[OrderController.create_order] Received data: {request.json}")
            req = CreateOrderRequest(**request.json)
            user_id = request.user_id
            result = order_service.create_order(req, user_id)
            return jsonify({'success': True, 'message': 'Tạo đơn hàng thành công', 'data': result}), 201
        except ValidationError as e:
            print(f"[OrderController.create_order] ValidationError: {e.errors()}")
            return jsonify({'success': False, 'message': 'Dữ liệu không hợp lệ', 'errors': e.errors()}), 400
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_my_orders(self):
        """Lấy danh sách đơn hàng của user"""
        try:
            user_id = request.user_id
            result = order_service.get_user_orders(user_id)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def filter_my_orders(self):
        """Lọc đơn hàng của user theo trạng thái"""
        try:
            user_id = request.user_id
            status = request.args.get('status')
            if not status:
                return jsonify({'success': False, 'message': 'Tham số status không được để trống'}), 400
            result = order_service.get_user_orders_by_status(user_id, status)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_order_detail(self, order_id: str):
        """Lấy chi tiết đơn hàng"""
        try:
            result = order_service.get_order_by_id(order_id)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 404
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def user_cancel_order(self, order_id: str):
        """User hủy đơn (PENDING only)"""
        try:
            user_id = request.user_id
            req = CancelOrderRequest(**request.json) if request.json else CancelOrderRequest()
            result = order_service.cancel_order(order_id, user_id, req.reason)
            return jsonify({'success': True, 'message': 'Hủy đơn hàng thành công', 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    # ==================== Shipper Routes ====================

    def get_pending_orders(self):
        """Shipper xem danh sách đơn chờ (PENDING)"""
        try:
            shipper_id = request.user_id
            result = order_service.get_pending_orders(shipper_id)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_my_deliveries(self):
        """Shipper xem danh sách đơn của mình"""
        try:
            shipper_id = request.user_id
            result = order_service.get_shipper_orders(shipper_id)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def accept_order(self, order_id: str):
        """Shipper nhận đơn (PENDING → SHIPPING)"""
        try:
            shipper_id = request.user_id
            result = order_service.accept_order(order_id, shipper_id)
            return jsonify({'success': True, 'message': 'Nhận đơn hàng thành công', 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def complete_delivery(self, order_id: str):
        """Shipper hoàn thành (SHIPPING → COMPLETED)"""
        try:
            shipper_id = request.user_id
            result = order_service.complete_order(order_id, shipper_id)
            return jsonify({'success': True, 'message': 'Hoàn thành đơn hàng thành công', 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def decline_order(self, order_id: str):
        """Shipper từ chối đơn PENDING (chưa nhận)"""
        try:
            shipper_id = request.user_id
            data = request.get_json() or {}
            reason = data.get('reason')
            result = order_service.decline_pending_order(order_id, shipper_id, reason)
            return jsonify({'success': True, 'message': 'Đã từ chối đơn hàng', 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def reject_order(self, order_id: str):
        """Shipper từ chối/hủy đơn đã nhận (SHIPPING → PENDING)"""
        try:
            shipper_id = request.user_id
            data = request.get_json() or {}
            reason = data.get('reason')
            result = order_service.reject_order_by_shipper(order_id, shipper_id, reason)
            return jsonify({'success': True, 'message': 'Từ chối đơn hàng thành công', 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_order_detail_for_shipper(self, order_id: str):
        """Shipper xem chi tiết đơn hàng"""
        try:
            shipper_id = request.user_id
            result = order_service.get_shipper_order_detail(order_id, shipper_id)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_deliveries_by_status(self):
        """Shipper lọc đơn hàng theo trạng thái"""
        try:
            shipper_id = request.user_id
            status = request.args.get('status')  # Query param: ?status=Shipping
            result = order_service.get_shipper_orders_by_status(shipper_id, status)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    # ==================== Admin Routes ====================

    def get_all_orders(self):
        """Admin xem tất cả đơn hàng"""
        try:
            result = order_service.get_all_orders()
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def filter_all_orders(self):
        """Admin lọc tất cả đơn hàng theo trạng thái"""
        try:
            status = request.args.get('status')
            if not status:
                return jsonify({'success': False, 'message': 'Tham số status không được để trống'}), 400
            result = order_service.get_all_orders_by_status(status)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def admin_cancel_order(self, order_id: str):
        """Admin hủy đơn (any status)"""
        try:
            req = CancelOrderRequest(**request.json) if request.json else CancelOrderRequest()
            result = order_service.admin_cancel_order(order_id, req.reason)
            return jsonify({'success': True, 'message': 'Hủy đơn hàng thành công', 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_restaurant_orders(self, restaurant_id: str):
        """Admin xem đơn hàng của nhà hàng"""
        try:
            result = order_service.get_restaurant_orders(restaurant_id)
            return jsonify({'success': True, 'data': result}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500


order_controller = OrderController()
