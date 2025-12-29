from typing import Optional, List, Dict
from datetime import datetime, date
from bson import ObjectId

from db.connection import vouchers_collection, orders_collection
from db.models.vouchers import Promotion, PromotionType
from db.models.order import OrderStatus
from utils.mongo_parser import parse_mongo_document


class VoucherService:
    """
    Voucher Service - Xử lý nghiệp vụ voucher/promotion
    
    Chức năng chính:
    - CRUD voucher (tạo, đọc, cập nhật, xóa)
    - Kiểm tra tính hợp lệ của voucher (eligibility)
    - Tính toán giảm giá (discount calculation)
    - Lấy danh sách voucher khả dụng cho user
    - Preview discount trước khi áp dụng
    """
    def __init__(self):
        self.collection = vouchers_collection

    # ============== Helpers ==============
    def _to_model(self, doc: dict) -> Promotion:
        """Chuyển MongoDB document thành Promotion model"""
        # Remove fields that are no longer in the model
        doc = doc.copy()  # Don't modify original
        doc.pop('usage_count', None)
        doc.pop('user_usage_history', None)
        doc.pop('updatedAt', None)
        doc.pop('updated_at', None)
        
        # Parse MongoDB Extended JSON format (dates, ObjectIds, etc.)
        doc = parse_mongo_document(doc)
        
        return Promotion(**doc)

    def _to_dict(self, promotion: Promotion) -> Dict:
        """Chuyển Promotion model thành dict để trả về API"""
        return promotion.to_dict()

    # ==================== MongoDB CRUD Operations ====================
    def create(self, data: Dict) -> Dict:
        """Tạo voucher mới - Input là dict từ schema, output là dict response"""
        promo = Promotion(
            code=data['code'],
            promo_name=data['promo_name'],
            type=data.get('type', PromotionType.PERCENT),
            value=float(data['value']),
            max_discount=float(data['max_discount']) if data.get('max_discount') is not None else None,
            min_order_amount=float(data['min_order_amount']) if data.get('min_order_amount') is not None else None,
            # Ưu tiên khóa 'restaurantId' (camelCase) từ controller; fallback 'restaurant_id' nếu có
            restaurant_id=(
                ObjectId(rid) if (rid := (data.get('restaurantId') or data.get('restaurant_id'))) else None
            ),
            first_order_only=bool(data.get('first_order_only', False)),
            active=bool(data.get('active', True)),
            start_date=data['start_date'],
            end_date=data['end_date'],
            description=data.get('description'),
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        result = self.collection.insert_one(promo.to_mongo())
        created = self.find_by_id(str(result.inserted_id))
        return self._to_dict(created)

    def update(self, promo_id: str, data: Dict) -> Dict:
        """Cập nhật voucher - Chỉ update các field được truyền lên"""
        updates: Dict = { 'updatedAt': datetime.now() }
        if 'promo_name' in data and data['promo_name'] is not None:
            updates['promo_name'] = data['promo_name']
        if 'type' in data and data['type'] is not None:
            updates['type'] = data['type'].value if isinstance(data['type'], PromotionType) else data['type']
        if 'value' in data and data['value'] is not None:
            updates['value'] = float(data['value'])
        if 'max_discount' in data:
            updates['max_discount'] = float(data['max_discount']) if data['max_discount'] is not None else None
        if 'min_order_amount' in data:
            updates['min_order_amount'] = float(data['min_order_amount']) if data['min_order_amount'] is not None else None
        # Hỗ trợ cả 'restaurantId' và 'restaurant_id' khi cập nhật
        if 'restaurantId' in data or 'restaurant_id' in data:
            rid = data.get('restaurantId') if 'restaurantId' in data else data.get('restaurant_id')
            updates['restaurantId'] = ObjectId(rid) if rid else None
        if 'first_order_only' in data and data['first_order_only'] is not None:
            updates['first_order_only'] = bool(data['first_order_only'])
        if 'active' in data and data['active'] is not None:
            updates['active'] = bool(data['active'])
        if 'start_date' in data and data['start_date'] is not None:
            updates['start_date'] = data['start_date']
        if 'end_date' in data and data['end_date'] is not None:
            updates['end_date'] = data['end_date']
        if 'description' in data:
            updates['description'] = data['description']

        self.collection.update_one({'_id': ObjectId(promo_id)}, {'$set': updates})
        updated = self.find_by_id(promo_id)
        if not updated:
            raise ValueError('Không tìm thấy voucher')
        return self._to_dict(updated)

    def delete(self, promo_id: str) -> None:
        """Xóa voucher khỏi database"""
        self.collection.delete_one({'_id': ObjectId(promo_id)})

    def find_by_id(self, promo_id: str) -> Optional[Promotion]:
        """Tìm voucher theo ID - Trả về Model"""
        doc = self.collection.find_one({'_id': ObjectId(promo_id)})
        return self._to_model(doc) if doc else None

    def find_by_code(self, code: str) -> Optional[Promotion]:
        """Tìm voucher theo mã code - Trả về Model"""
        doc = self.collection.find_one({'code': code})
        return self._to_model(doc) if doc else None

    def list_all(self) -> List[Dict]:
        """Lấy tất cả voucher (Admin) - Trả về list dict"""
        result = []
        # Sort by createdAt (camelCase) or created_at (snake_case) - try both
        try:
            cursor = self.collection.find().sort('createdAt', -1)
        except:
            try:
                cursor = self.collection.find().sort('created_at', -1)
            except:
                cursor = self.collection.find()
        
        total_docs = 0
        for doc in cursor:
            total_docs += 1
            try:
                result.append(self._to_dict(self._to_model(doc)))
            except Exception as e:
                # Log error nhưng tiếp tục với document tiếp theo
                promo_id = doc.get('_id', 'unknown')
                print(f"Error parsing voucher {promo_id}: {e}")
                print(f"  Document keys: {list(doc.keys())}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"list_all: Found {total_docs} documents, returning {len(result)} vouchers")
        return result

    # ==================== Business Logic ====================
    def _is_date_active(self, promo: Promotion, now: Optional[datetime] = None) -> bool:
        """Kiểm tra voucher có đang trong thời gian hiệu lực không"""
        now = now or datetime.now()
        return (promo.start_date <= now) and (now <= promo.end_date)

    def _is_first_order_eligible(self, user_id: str) -> bool:
        """
        Kiểm tra user có thể dùng voucher first_order_only không.
        Logic mới: Chỉ khi user đã từng áp dụng voucher first_order_only thì mới không được dùng.
        - Nếu user chưa từng dùng voucher first_order_only nào → True (có thể dùng)
        - Nếu user đã dùng voucher first_order_only nào đó → False (không thể dùng)
        - Chỉ đếm đơn hàng không bị hủy (status != 'Cancelled')
        """
        try:
            user_oid = ObjectId(user_id)
            
            # Tìm tất cả đơn hàng của user có sử dụng voucher (trừ đơn bị hủy)
            orders_with_voucher = orders_collection.find({
                'userId': user_oid,
                'promoId': {'$ne': None},  # Có voucher
                'status': {'$ne': 'Cancelled'}  # Không bị hủy
            }, {'promoId': 1})  # Chỉ lấy promoId để tối ưu
            
            # Kiểm tra từng voucher xem có first_order_only không
            for order in orders_with_voucher:
                promo_id = order.get('promoId')
                if not promo_id:
                    continue
                
                # Tìm voucher tương ứng
                promo = self.find_by_id(str(promo_id))
                if promo and promo.first_order_only:
                    # User đã từng dùng voucher first_order_only
                    return False
            
            # User chưa từng dùng voucher first_order_only nào
            return True
        except Exception as e:
            print(f"Error in _is_first_order_eligible: {e}")
            import traceback
            traceback.print_exc()
            # Nếu có lỗi, trả về False để an toàn (không cho dùng)
            return False

    def _has_user_used_voucher(self, promo_id: str, user_id: str) -> bool:
        """
        Kiểm tra user đã sử dụng voucher này chưa.
        Query orders collection để tìm đơn hàng có promoId này và status không phải CANCELLED.
        """
        try:
            if not promo_id:
                return False
            
            # Convert to ObjectId
            try:
                promo_oid = ObjectId(promo_id)
                user_oid = ObjectId(user_id)
            except Exception as e:
                print(f"Error converting IDs to ObjectId: {e}")
                return False
            
            # Query orders collection
            query = {
                'userId': user_oid,
                'promoId': promo_oid,
                'status': {'$ne': 'Cancelled'}  # Chỉ đếm đơn không bị hủy
            }
            
            count = orders_collection.count_documents(query)
            return count > 0
        except Exception as e:
            print(f"Error in _has_user_used_voucher: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _restaurant_scope_ok(self, promo: Promotion, restaurant_id: str) -> bool:
        """Kiểm tra voucher có áp dụng cho nhà hàng này không"""
        if promo.restaurant_id is None:
            return True
        return str(promo.restaurant_id) == restaurant_id

    def calculate_discount(self, promo: Promotion, subtotal: float, shipping_fee: float) -> float:
        """
        Tính số tiền giảm dựa trên loại voucher:
        - PERCENT: Giảm % trên subtotal, giới hạn bởi max_discount
        - FIXED: Giảm số tiền cố định (không vượt quá subtotal)
        - FREESHIP: Giảm phí ship, giới hạn bởi max_discount hoặc shipping_fee
        """
        discount = 0.0
        if promo.type == PromotionType.PERCENT:
            discount = subtotal * (float(promo.value) / 100.0)
            if promo.max_discount is not None:
                discount = min(discount, float(promo.max_discount))
        elif promo.type == PromotionType.FIXED:
            discount = min(float(promo.value), subtotal)
        elif promo.type == PromotionType.FREESHIP:
            cap = float(promo.max_discount) if promo.max_discount is not None else shipping_fee
            discount = min(shipping_fee, cap)
        return float(max(discount, 0.0))

    def validate_and_calculate(self, user_id: str, restaurant_id: str, subtotal: float, shipping_fee: float, promo: Promotion) -> float:
        """
        Validate voucher và tính discount
        - Kiểm tra voucher active
        - Kiểm tra thời gian hiệu lực
        - Kiểm tra phạm vi nhà hàng
        - Kiểm tra giá trị đơn tối thiểu
        - Kiểm tra điều kiện đơn đầu tiên
        - Kiểm tra user đã dùng voucher này chưa
        Raise ValueError nếu không hợp lệ, return discount nếu OK
        """
        if not promo.active:
            raise ValueError('Voucher không hoạt động')
        if not self._is_date_active(promo):
            raise ValueError('Voucher không nằm trong thời gian hiệu lực')
        if not self._restaurant_scope_ok(promo, restaurant_id):
            raise ValueError('Voucher không áp dụng cho nhà hàng này')
        if promo.min_order_amount is not None and subtotal < float(promo.min_order_amount):
            raise ValueError('Chưa đạt giá trị tối thiểu để áp dụng voucher')
        if promo.first_order_only and not self._is_first_order_eligible(user_id):
            raise ValueError('Voucher chỉ áp dụng cho đơn đầu tiên')
        # Kiểm tra user đã dùng voucher này chưa
        if self._has_user_used_voucher(str(promo.promo_id), user_id):
            raise ValueError('Bạn đã sử dụng voucher này rồi')
        return self.calculate_discount(promo, subtotal, shipping_fee)

    def get_available_for_user(self, user_id: str, restaurant_id: Optional[str] = None) -> List[Dict]:
        """
        Lấy danh sách voucher khả dụng cho user
        - Chỉ lấy voucher đang active
        - Trong thời gian hiệu lực
        - Áp dụng cho nhà hàng (nếu filter theo restaurantId) hoặc toàn hệ thống
        - Loại bỏ voucher user đã sử dụng rồi
        - Trả về thêm flag eligible_first_order để client biết voucher first order
        """
        # Use UTC time to match ISO date strings from JSON
        from datetime import timezone
        now = datetime.now(timezone.utc)
        user_oid = ObjectId(user_id)
        query: Dict = {
            'active': True,
            'start_date': { '$lte': now },
            'end_date': { '$gte': now }
        }
        if restaurant_id:
            query['$or'] = [
                { 'restaurantId': None },
                { 'restaurantId': ObjectId(restaurant_id) }
            ]
        
        print(f"get_available_for_user: Query: {query}")
        cursor = self.collection.find(query).sort('createdAt', -1)
        total_found = self.collection.count_documents(query)
        print(f"get_available_for_user: Found {total_found} vouchers matching query")
        
        result: List[Dict] = []
        # Kiểm tra xem user đã từng dùng voucher first_order_only chưa
        can_use_first_order = self._is_first_order_eligible(user_id)
        
        for doc in cursor:
            try:
                promo = self._to_model(doc)
                # Kiểm tra user đã dùng voucher này chưa
                if self._has_user_used_voucher(str(promo.promo_id), user_id):
                    continue  # Bỏ qua voucher user đã dùng
                
                # Nếu user đã dùng voucher first_order_only nào đó, thì không hiển thị voucher first_order_only nữa
                if promo.first_order_only and not can_use_first_order:
                    continue  # Bỏ qua voucher first_order_only nếu user đã dùng voucher first_order_only rồi
                
                item = promo.to_dict()
                # Set eligible_first_order:
                # - If voucher is NOT first_order_only: always True (no restriction)
                # - If voucher IS first_order_only: True only if user hasn't used any first_order_only voucher yet
                if promo.first_order_only:
                    item['eligible_first_order'] = can_use_first_order
                else:
                    item['eligible_first_order'] = True  # No first-order restriction
                result.append(item)
            except Exception as e:
                print(f"Error processing voucher {doc.get('_id', 'unknown')}: {e}")
                continue
        
        print(f"get_available_for_user: Returning {len(result)} vouchers (filtered out used vouchers)")
        return result

    def get_expired_for_user(self, user_id: Optional[str], restaurant_id: Optional[str] = None) -> List[Dict]:
        """
        Lấy danh sách voucher đã hết hạn cho user
        - Voucher không active hoặc end_date < now
        - Hoặc voucher user đã sử dụng rồi (nếu có user_id)
        - Áp dụng cho nhà hàng (nếu filter) hoặc toàn hệ thống
        """
        # Use UTC time to match ISO date strings from JSON
        from datetime import timezone
        now = datetime.now(timezone.utc)
        
        # Build restaurant filter
        restaurant_filter = []
        if restaurant_id:
            restaurant_filter = [
                {'restaurantId': None},
                {'restaurantId': ObjectId(restaurant_id)}
            ]
        
        # Query 1: Voucher hết hạn hoặc inactive
        expired_conditions = [
            {'active': False},
            {'end_date': {'$lt': now}}
        ]
        
        # Combine với restaurant filter nếu có
        if restaurant_filter:
            expired_query = {
                '$and': [
                    {'$or': restaurant_filter},
                    {'$or': expired_conditions}
                ]
            }
        else:
            expired_query = {'$or': expired_conditions}
        
        result: List[Dict] = []
        seen_ids = set()
        
        # Lấy voucher hết hạn
        print(f"get_expired_for_user: Query: {expired_query}")
        total_found = self.collection.count_documents(expired_query)
        print(f"get_expired_for_user: Found {total_found} expired vouchers")
        
        for doc in self.collection.find(expired_query).sort('createdAt', -1):
            try:
                promo = self._to_model(doc)
                if promo.promo_id not in seen_ids:
                    result.append(promo.to_dict())
                    seen_ids.add(promo.promo_id)
            except Exception as e:
                print(f"Error processing expired voucher {doc.get('_id', 'unknown')}: {e}")
                continue
        
        print(f"get_expired_for_user: Returning {len(result)} expired vouchers")
        
        # Query 2: Voucher user đã dùng (removed - user_usage_history no longer exists)
        # if user_id:
        #     used_query = {'user_usage_history.user_id': ObjectId(user_id)}
        #     ...
        
        return result

    def get_all_available(self, restaurant_id: Optional[str] = None) -> List[Dict]:
        """
        Admin lấy danh sách tất cả voucher khả dụng (không filter user)
        - Voucher active
        - Trong thời gian hiệu lực
        - Áp dụng cho nhà hàng (nếu filter) hoặc toàn hệ thống
        """
        now = datetime.now()
        query: Dict = {
            'active': True,
            'start_date': { '$lte': now },
            'end_date': { '$gte': now }
        }
        if restaurant_id:
            query['$or'] = [
                { 'restaurantId': None },
                { 'restaurantId': ObjectId(restaurant_id) }
            ]
        cursor = self.collection.find(query).sort('createdAt', -1)
        result: List[Dict] = []
        for doc in cursor:
            promo = self._to_model(doc)
            result.append(promo.to_dict())
        return result

    def preview_discount(self, user_id: str, restaurant_id: str, subtotal: float, shipping_fee: float, promo_id: Optional[str] = None, code: Optional[str] = None) -> Dict:
        """
        Preview discount trước khi apply voucher vào order
        - Tìm voucher theo promo_id hoặc code
        - Validate và tính discount
        - Trả về thông tin voucher + discount + total sau khi áp dụng
        """
        promo: Optional[Promotion] = None
        if promo_id:
            promo = self.find_by_id(promo_id)
        elif code:
            promo = self.find_by_code(code)
        if not promo:
            raise ValueError('Không tìm thấy voucher')
        discount = self.validate_and_calculate(user_id, restaurant_id, subtotal, shipping_fee, promo)
        return {
            'promotion': promo.to_dict(),
            'discount': float(discount),
            'subtotal': float(subtotal),
            'shipping_fee': float(shipping_fee),
            'total_after_discount': float(max(subtotal + shipping_fee - discount, 0.0))
        }

    def mark_voucher_used(self, promo_id: str, user_id: str) -> None:
        """
        Ghi nhận voucher đã được sử dụng bởi user cụ thể
        (Tracking fields removed - no action needed)
        """
        pass  # Tracking fields removed, no action needed

    def refund_voucher_used(self, promo_id: str, user_id: str) -> None:
        """
        Hoàn lại voucher đã đánh dấu sử dụng khi đơn hàng bị hủy.
        (Tracking fields removed - no action needed)
        """
        pass  # Tracking fields removed, no action needed



voucher_service = VoucherService()
