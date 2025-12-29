from flask import request, jsonify
from services.payment_service import payment_service
from schemas.payment_schema import PaymentResponse


class PaymentController:
    def get_payment(self, payment_id: str):
        try:
            data = payment_service.get_payment(payment_id)
            resp = PaymentResponse(**data).model_dump(by_alias=True)
            return jsonify({'success': True, 'data': resp}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 404
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_by_order(self, order_id: str):
        try:
            data = payment_service.get_payment_by_order(order_id)
            resp = PaymentResponse(**data).model_dump(by_alias=True)
            return jsonify({'success': True, 'data': resp}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 404
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def get_my_payments(self):
        try:
            user_id = request.user_id
            status = request.args.get('status')
            data = payment_service.get_my_payments(user_id, status)
            resp = [PaymentResponse(**p).model_dump(by_alias=True) for p in data]
            return jsonify({'success': True, 'data': resp}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def mark_paid(self, payment_id: str):
        try:
            data = payment_service.mark_paid(payment_id)
            resp = PaymentResponse(**data).model_dump(by_alias=True)
            return jsonify({'success': True, 'message': 'Cập nhật thanh toán thành công', 'data': resp}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def mark_failed(self, payment_id: str):
        try:
            data = payment_service.mark_failed(payment_id)
            resp = PaymentResponse(**data).model_dump(by_alias=True)
            return jsonify({'success': True, 'message': 'Đánh dấu thanh toán thất bại', 'data': resp}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500

    def refund(self, payment_id: str):
        try:
            data = payment_service.refund(payment_id)
            resp = PaymentResponse(**data).model_dump(by_alias=True)
            return jsonify({'success': True, 'message': 'Hoàn tiền thành công', 'data': resp}), 200
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 400
        except Exception as e:
            return jsonify({'success': False, 'message': f'Lỗi server: {str(e)}'}), 500


payment_controller = PaymentController()
