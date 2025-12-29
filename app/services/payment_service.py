from typing import Optional, List, Dict
from datetime import datetime
from bson import ObjectId

from db.connection import payments_collection, orders_collection
from db.models.payment import Payment, PaymentStatus, PaymentMethod
from utils.mongo_parser import parse_mongo_document


class PaymentService:
    """Service xử lý nghiệp vụ payment"""

    def __init__(self, user_service=None):
        self.collection = payments_collection
        if user_service is None:
            from services.user_service import user_service as us
            self.user_service = us
        else:
            self.user_service = user_service

    # ==================== Helpers ====================
    def _to_model(self, doc: dict) -> Payment:
        """Chuyển Mongo document thành model Payment"""
        # Parse MongoDB Extended JSON format
        doc = parse_mongo_document(doc)
        return Payment(**doc)

    def _to_dict(self, payment: Payment) -> Dict:
        """Chuyển Payment model thành dict trả về API"""
        return payment.to_dict()

        # ==================== LAYER 1: MongoDB CRUD Operations ====================
    def create_payment(self, order_id: str, user_id: str, amount: float, method: PaymentMethod, status: PaymentStatus) -> Payment:
        """Tạo payment mới cho đơn hàng"""
        payment = Payment(
            order_id=ObjectId(order_id),
            user_id=ObjectId(user_id),
            amount=amount,
            method=method,
            status=status,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        result = self.collection.insert_one(payment.to_mongo())
        payment.payment_id = result.inserted_id
        return payment

    def delete_payment(self, payment_id: str) -> None:
        """Xóa payment (dùng cho rollback)"""
        self.collection.delete_one({'_id': ObjectId(payment_id)})

    def find_by_id(self, payment_id: str) -> Optional[Payment]:
        """Tìm payment theo ID"""
        doc = self.collection.find_one({'_id': ObjectId(payment_id)})
        return self._to_model(doc) if doc else None

    def find_by_order_id(self, order_id: str) -> Optional[Payment]:
        """Tìm payment theo order"""
        doc = self.collection.find_one({'orderId': ObjectId(order_id)})
        return self._to_model(doc) if doc else None

    def find_by_user_id(self, user_id: str, status: Optional[str] = None) -> List[Payment]:
        """Lấy danh sách payment của user (có filter status nếu truyền)"""
        query: Dict = {'userId': ObjectId(user_id)}
        if status:
            query['status'] = status
        cursor = self.collection.find(query).sort('createdAt', -1)
        return [self._to_model(doc) for doc in cursor]

    # ==================== LAYER 2: Business Logic ====================
    def get_payment(self, payment_id: str) -> Dict:
        """Lấy chi tiết payment"""
        payment = self.find_by_id(payment_id)
        if not payment:
            raise ValueError('Không tìm thấy payment')
        return self._to_dict(payment)

    def get_payment_by_order(self, order_id: str) -> Dict:
        """Lấy payment theo order"""
        payment = self.find_by_order_id(order_id)
        if not payment:
            raise ValueError('Không tìm thấy payment của đơn hàng')
        return self._to_dict(payment)

    def get_my_payments(self, user_id: str, status: Optional[str] = None) -> List[Dict]:
        """Lấy danh sách payment của user (có thể lọc status)"""
        payments = self.find_by_user_id(user_id, status)
        return [self._to_dict(p) for p in payments]

    def mark_paid(self, payment_id: str) -> Dict:
        """Đánh dấu đã thanh toán"""
        payment = self.find_by_id(payment_id)
        if not payment:
            raise ValueError('Không tìm thấy payment')

        # Đã Paid thì trả về luôn
        if payment.status == PaymentStatus.PAID:
            return self._to_dict(payment)

        self.collection.update_one(
            {'_id': payment.id},
            {'$set': {'status': PaymentStatus.PAID.value, 'updatedAt': datetime.now()}}
        )
        updated = self.find_by_id(payment_id)
        return self._to_dict(updated)

    def mark_failed(self, payment_id: str) -> Dict:
        """Đánh dấu thanh toán thất bại"""
        payment = self.find_by_id(payment_id)
        if not payment:
            raise ValueError('Không tìm thấy payment')

        self.collection.update_one(
            {'_id': payment.id},
            {'$set': {'status': PaymentStatus.FAILED.value, 'updatedAt': datetime.now()}}
        )
        updated = self.find_by_id(payment_id)
        return self._to_dict(updated)

    def refund(self, payment_id: str) -> Dict:
        """Hoàn tiền (chỉ cho payment đã Paid)"""
        payment = self.find_by_id(payment_id)
        if not payment:
            raise ValueError('Không tìm thấy payment')

        # Đã hoàn rồi thì trả về luôn
        if payment.status == PaymentStatus.REFUNDED:
            return self._to_dict(payment)

        # Chỉ hoàn khi đã thanh toán
        if payment.status != PaymentStatus.PAID:
            raise ValueError('Chỉ có thể hoàn tiền cho payment đã thanh toán')

        # Cộng tiền lại vào balance user
        self.user_service.credit_balance(str(payment.user_id), float(payment.amount))

        # Cập nhật payment status + timestamp
        self.collection.update_one(
            {'_id': payment.id},
            {'$set': {'status': PaymentStatus.REFUNDED.value, 'updatedAt': datetime.now()}}
        )

        # Cập nhật đơn hàng: đánh dấu đã hoàn tiền
        orders_collection.update_one(
            {'_id': ObjectId(str(payment.order_id))},
            {'$set': {
                'refunded': True,
                'refunded_amount': float(payment.amount),
                'refund_at': datetime.now(),
                'updatedAt': datetime.now()
            }}
        )

        updated = self.find_by_id(payment_id)
        return self._to_dict(updated)
payment_service = PaymentService()
