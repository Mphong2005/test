from typing import Optional, List, Dict
from datetime import datetime
from bson import ObjectId
from pymongo.collection import Collection

from db.connection import orders_collection
from db.models.order import Order, OrderItem, OrderStatus
from db.models.payment import PaymentMethod, PaymentStatus
from services.voucher_service import voucher_service
from services.payment_service import payment_service
from utils.mongo_parser import parse_mongo_document
from schemas.order_schema import (
    CreateOrderRequest,
    UpdateOrderStatusRequest,
    CancelOrderRequest,
    AssignShipperRequest,
    OrderResponse,
    OrderSimpleResponse,
)


class OrderService:
    def __init__(self, restaurant_service=None, user_service=None):
        self.collection: Collection = orders_collection
        # Import here to avoid circular dependency
        if restaurant_service is None:
            from services.restaurant_service import restaurant_service as rs
            self.restaurant_service = rs
        else:
            self.restaurant_service = restaurant_service
        
        if user_service is None:
            from services.user_service import user_service as us
            self.user_service = us
        else:
            self.user_service = user_service

    # ==================== Helpers ====================
    def _to_model(self, doc: dict) -> Order:
        """Convert MongoDB doc to Order model"""
        # Parse MongoDB Extended JSON format
        doc = parse_mongo_document(doc)
        return Order(**doc)

    def _to_simple_response(self, order: Order) -> Dict:
        """Convert Order model to simple response dict"""
        data = order.to_dict()
        
        # Thêm foodName (tên món đầu tiên) để frontend hiển thị
        if order.items and len(order.items) > 0:
            data['foodName'] = order.items[0].food_name
        
        # Thêm imageUrl từ món ăn đầu tiên trong order
        try:
            if order.items and len(order.items) > 0:
                first_food_name = order.items[0].food_name
                # Lấy hình ảnh từ restaurant menu
                restaurant = self.restaurant_service.find_by_id(str(order.restaurant_id))
                if restaurant and restaurant.menu:
                    # Normalize food name for comparison (remove extra spaces, lowercase)
                    normalized_order_name = first_food_name.lower().strip()
                    
                    for category in restaurant.menu:
                        for food_item in category.items:
                            # Normalize menu item name for comparison
                            normalized_menu_name = food_item.name.lower().strip()
                            
                            # Exact match or contains match
                            if normalized_menu_name == normalized_order_name or normalized_order_name in normalized_menu_name or normalized_menu_name in normalized_order_name:
                                # Get image from food_item
                                food_image = None
                                if hasattr(food_item, 'image') and food_item.image:
                                    food_image = food_item.image
                                elif hasattr(food_item, 'imageUrl') and food_item.imageUrl:
                                    food_image = food_item.imageUrl
                                
                                if food_image:
                                    data['imageUrl'] = food_image
                                    break
                        if 'imageUrl' in data:
                            break
        except Exception as e:
            # Log error for debugging but don't fail the response
            print(f"Error getting image for order {order.order_id}: {e}")
            pass
        
        # Bổ sung thông tin user (email) - để shipper có thể liên hệ
        try:
            if order.user_id:
                user = self.user_service.find_by_id(str(order.user_id))
                if user:
                    data['userEmail'] = user.email
        except Exception:
            # Không chặn response nếu lấy user info lỗi
            pass
        
        # Bổ sung thông tin shipper (nếu có) - giống như _to_full_response
        try:
            if order.shipper_id:
                shipper = self.user_service.find_by_id(str(order.shipper_id))
                if shipper:
                    data['shipper'] = {
                        'shipperId': str(order.shipper_id),
                        'fullname': shipper.fullname,
                        'phone_number': shipper.phone_number,
                        'email': shipper.email
                    }
        except Exception:
            # Không chặn response nếu lấy shipper info lỗi
            pass
        
        return OrderSimpleResponse(**data).model_dump(by_alias=True)

    def _to_full_response(self, order: Order) -> Dict:
        """Convert Order model to full response dict + shipper details if available"""
        data = order.to_dict()

        # Bổ sung thông tin shipper (nếu có)
        try:
            if order.shipper_id:
                shipper = self.user_service.find_by_id(str(order.shipper_id))
                if shipper:
                    data['shipper'] = {
                        'shipperId': str(order.shipper_id),
                        'fullname': shipper.fullname,
                        'phone_number': shipper.phone_number
                    }
        except Exception:
            # Không chặn response nếu lấy shipper info lỗi
            pass

        return OrderResponse(**data).model_dump(by_alias=True)

    # ==================== LAYER 1: MongoDB CRUD Operations ====================

    def find_by_id(self, order_id: str) -> Optional[Order]:
        """Tìm đơn hàng theo ID"""
        try:
            doc = self.collection.find_one({'_id': ObjectId(order_id)})
            return self._to_model(doc) if doc else None
        except Exception as e:
            print(f"Error finding order by id: {e}")
            return None

    def find_by_user_id(self, user_id: str) -> List[Order]:
        """Tìm tất cả đơn hàng của user"""
        try:
            result = []
            for doc in self.collection.find({'userId': ObjectId(user_id)}):
                result.append(self._to_model(doc))
            return result
        except Exception as e:
            print(f"Error finding orders by user id: {e}")
            return []

    def find_by_restaurant_id(self, restaurant_id: str) -> List[Order]:
        """Tìm tất cả đơn hàng cho nhà hàng"""
        try:
            result = []
            for doc in self.collection.find({'restaurantId': ObjectId(restaurant_id)}):
                result.append(self._to_model(doc))
            return result
        except Exception as e:
            print(f"Error finding orders by restaurant id: {e}")
            return []

    def find_by_shipper_id(self, shipper_id: str) -> List[Order]:
        """Tìm tất cả đơn hàng của shipper"""
        try:
            # Validate shipper_id
            if not shipper_id:
                print("Warning: shipper_id is empty")
                return []
            
            # Convert to ObjectId and query - only return orders where shipperId is not None
            result = []
            query = {'shipperId': ObjectId(shipper_id)}
            for doc in self.collection.find(query):
                # Double check that the order actually belongs to this shipper
                order = self._to_model(doc)
                if order.shipper_id and str(order.shipper_id) == shipper_id:
                    result.append(order)
                else:
                    # This shouldn't happen, but log it if it does
                    print(f"Warning: Order {order.id} has shipperId {order.shipper_id} but query was for {shipper_id}")
            return result
        except Exception as e:
            print(f"Error finding orders by shipper id: {e}")
            return []

    def find_by_status(self, status: str) -> List[Order]:
        """Tìm đơn hàng theo trạng thái"""
        try:
            result = []
            for doc in self.collection.find({'status': status}):
                result.append(self._to_model(doc))
            return result
        except Exception as e:
            print(f"Error finding orders by status: {e}")
            return []

    def find_all(self) -> List[Order]:
        """Lấy tất cả đơn hàng"""
        result = []
        for doc in self.collection.find().sort('createdAt', -1):
            try:
                result.append(self._to_model(doc))
            except Exception as e:
                # Log error nhưng tiếp tục với document tiếp theo
                order_id = doc.get('_id', 'unknown')
                print(f"Error parsing order {order_id}: {e}")
                print(f"  Document: {doc}")
                continue
        return result

    def create_order_in_db(self, req: CreateOrderRequest, user_id: str) -> Optional[Order]:
        """Tạo đơn hàng mới - Lấy giá từ DB, lưu thông tin denormalized cho shipper"""
        try:
            # Kiểm tra items không trống
            if not req.items or len(req.items) == 0:
                raise ValueError('Đơn hàng phải có ít nhất 1 món')
            
            # ===== Lấy thông tin User (fullname, phone) =====
            user = self.user_service.find_by_id(user_id)
            if not user:
                raise ValueError(f'Không tìm thấy user {user_id}')
            
            user_fullname = user.fullname or "Unknown"
            user_phone = user.phone_number or "Unknown"
            
            # ===== Lấy thông tin Restaurant (name, address, hotline) =====
            restaurant = self.restaurant_service.find_by_id(req.restaurant_id)
            if not restaurant:
                raise ValueError(f'Không tìm thấy nhà hàng {req.restaurant_id}')
            
            restaurant_name = restaurant.restaurant_name
            restaurant_address = restaurant.address or "Unknown"
            restaurant_hotline = restaurant.hotline
            
            # ===== Build price map từ menu (Tối ưu: Tránh N+1 query) =====
            price_map = {}  # {food_name_lower: price}
            for category in restaurant.menu:
                for food_item in category.items:
                    price_map[food_item.name.lower()] = float(food_item.price)
            
            # ===== Lấy giá từ price_map (O(1) lookup thay vì N queries) =====
            items_list: List[OrderItem] = []
            subtotal = 0
            
            for item_req in req.items:
                # Lookup giá từ price_map (đã build sẵn)
                food_name_lower = item_req.food_name.lower()
                if food_name_lower not in price_map:
                    raise ValueError(f'Món "{item_req.food_name}" không có trong menu nhà hàng')
                
                unit_price = price_map[food_name_lower]
                item_subtotal = item_req.quantity * unit_price
                
                item = OrderItem(
                    food_name=item_req.food_name,
                    quantity=item_req.quantity,
                    unit_price=unit_price,  # Từ DB, không phải frontend
                    subtotal=item_subtotal
                )
                items_list.append(item)
                subtotal += item_subtotal
            
            # Áp dụng voucher (nếu có) để tính discount server-side
            discount = 0.0
            if req.promo_id:
                try:
                    preview_result = voucher_service.preview_discount(
                        user_id=user_id,
                        restaurant_id=req.restaurant_id,
                        subtotal=subtotal,
                        shipping_fee=req.shipping_fee,
                        promo_id=req.promo_id
                    )
                    discount = preview_result['discount']
                    print(f"[DEBUG] Voucher applied - promo_id: {req.promo_id}, discount: {discount}, subtotal: {subtotal}, shipping_fee: {req.shipping_fee}")
                except Exception as e:
                    print(f"[ERROR] Failed to apply voucher {req.promo_id}: {str(e)}")
                    raise ValueError(f'Không thể áp dụng voucher: {str(e)}')

            # Tính total_amount
            total_amount = subtotal + req.shipping_fee - discount
            print(f"[DEBUG] Order calculation - subtotal: {subtotal}, shipping_fee: {req.shipping_fee}, discount: {discount}, total_amount: {total_amount}")
            
            # ===== Tạo Order object với denormalized data =====
            order = Order(
                user_id=ObjectId(user_id),
                restaurant_id=ObjectId(req.restaurant_id),
                user_fullname=user_fullname,  # Denormalized
                user_phone=user_phone,  # Denormalized
                restaurant_name=restaurant_name,  # Denormalized
                restaurant_address=restaurant_address,  # Denormalized
                restaurant_hotline=restaurant_hotline,  # Denormalized
                items=items_list,
                address=req.address,
                note=req.note,
                subtotal=subtotal,
                shipping_fee=req.shipping_fee,
                discount=discount,
                total_amount=total_amount,
                promo_id=ObjectId(req.promo_id) if req.promo_id else None,
                payment_method=req.payment_method,
                status=OrderStatus.PENDING,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            
            # Insert vào MongoDB
            order_doc = order.to_mongo()
            print(f"[DEBUG] Saving order to MongoDB - discount: {order_doc.get('discount')}, total_amount: {order_doc.get('total_amount')}, promoId: {order_doc.get('promoId')}")
            insert_result = self.collection.insert_one(order_doc)
            created = self.find_by_id(str(insert_result.inserted_id))
            
            # Verify the order was saved correctly
            if created:
                print(f"[DEBUG] Order created successfully - ID: {created.id}, discount: {created.discount}, total_amount: {created.total_amount}, promoId: {created.promo_id}")
            else:
                print(f"[ERROR] Failed to retrieve created order")
            
            # NOTE: Không mark voucher ở đây để tránh mất voucher khi payment fail
            # Voucher sẽ được mark trong create_order() sau khi payment thành công
            
            return created
        except Exception as e:
            print(f"Error creating order: {e}")
            raise ValueError(f'Lỗi DB khi tạo đơn hàng: {str(e)}')

    def update_order_status_in_db(self, order_id: str, new_status: str, shipper_id: Optional[str] = None) -> Optional[Order]:
        """Update trạng thái đơn hàng"""
        try:
            update_data = {
                'status': new_status,
                'updatedAt': datetime.now()
            }
            
            # Nếu shipper nhận đơn
            if shipper_id and new_status == OrderStatus.SHIPPING.value:
                update_data['shipperId'] = ObjectId(shipper_id)
            
            result = self.collection.update_one(
                {'_id': ObjectId(order_id)},
                {'$set': update_data}
            )
            
            return self.find_by_id(order_id) if result.matched_count > 0 else None
        except Exception as e:
            raise ValueError(f'Lỗi DB khi cập nhật trạng thái: {str(e)}')

    def cancel_order_in_db(self, order_id: str, cancelled_by: str, reason: Optional[str] = None) -> Optional[Order]:
        """Hủy đơn hàng"""
        try:
            update_data = {
                'status': OrderStatus.CANCELLED.value,
                'cancelled_by': cancelled_by,
                'cancellation_reason': reason,
                'updatedAt': datetime.now()
            }
            
            result = self.collection.update_one(
                {'_id': ObjectId(order_id)},
                {'$set': update_data}
            )
            
            return self.find_by_id(order_id) if result.matched_count > 0 else None
        except Exception as e:
            raise ValueError(f'Lỗi DB khi hủy đơn hàng: {str(e)}')

    # ==================== LAYER 2: Business Logic ====================

    def create_order(self, req: CreateOrderRequest, user_id: str) -> Dict:
        """Tạo đơn hàng mới (User flow) - Giá từ DB, xử lý thanh toán theo phương thức"""
        created = None
        try:
            # Kiểm tra items không trống
            if not req.items or len(req.items) == 0:
                raise ValueError('Đơn hàng phải có ít nhất 1 món')
            
            # BƯỚC 1: Tạo order (giá sẽ được lấy từ DB bên trong create_order_in_db)
            created = self.create_order_in_db(req, user_id)
            if not created:
                raise ValueError('Không thể tạo đơn hàng')

            # BƯỚC 2: Tạo payment và xử lý thanh toán theo phương thức
            payment_status = PaymentStatus.PENDING
            payment = None
            try:
                print(f"[DEBUG] Payment method: {req.payment_method}")
                
                # CHỈ TRỪ BALANCE KHI PAYMENT METHOD LÀ BALANCE
                if req.payment_method == PaymentMethod.BALANCE:
                    # Kiểm tra và trừ số dư
                    print(f"[DEBUG] Deducting balance - user_id: {user_id}, amount: {created.total_amount}, order_id: {created.id}")
                    self.user_service.deduct_balance(user_id, created.total_amount)
                    payment_status = PaymentStatus.PAID
                    print(f"[DEBUG] Balance deducted successfully")
                elif req.payment_method == PaymentMethod.COD:
                    # COD: Không trừ balance, payment_status = PENDING
                    print(f"[DEBUG] COD payment - No balance deduction")
                    payment_status = PaymentStatus.PENDING
                else:
                    print(f"[WARNING] Unknown payment method: {req.payment_method}")
                    payment_status = PaymentStatus.PENDING

                payment = payment_service.create_payment(
                    order_id=str(created.id),
                    user_id=user_id,
                    amount=created.total_amount,
                    method=req.payment_method,
                    status=payment_status
                )

                # Gắn paymentId vào order
                self.collection.update_one(
                    {'_id': ObjectId(str(created.id))},
                    {'$set': {'paymentId': payment.id}}
                )
                
                # ✅ CHỈ mark voucher SAU KHI payment thành công
                # Đảm bảo nếu payment fail, voucher không bị mất
                if req.promo_id:
                    try:
                        voucher_service.mark_voucher_used(req.promo_id, user_id)
                    except Exception as e:
                        print(f"Warning: Could not mark voucher as used: {e}")
                        # Không chặn flow nếu mark voucher fail (đơn hàng vẫn thành công)
            except Exception as e:
                # ROLLBACK: Xóa payment (nếu có) và order
                if payment and payment.id:
                    try:
                        payment_service.delete_payment(str(payment.id))
                    except:
                        pass
                if created:
                    try:
                        self.collection.delete_one({'_id': ObjectId(str(created.id))})
                    except:
                        pass
                raise ValueError(f'Thanh toán thất bại: {str(e)}')
            
            return self._to_full_response(created)
        except ValueError:
            raise
        except Exception as e:
            # Nếu có lỗi khác và đã tạo order, cũng rollback
            if created:
                try:
                    self.collection.delete_one({'_id': ObjectId(str(created.id))})
                except:
                    pass  # Ignore rollback error
            raise ValueError(f'Lỗi khi tạo đơn hàng: {str(e)}')

    def get_order_by_id(self, order_id: str) -> Dict:
        """Lấy chi tiết đơn hàng"""
        order = self.find_by_id(order_id)
        if not order:
            raise ValueError('Không tìm thấy đơn hàng')
        
        return self._to_full_response(order)

    def get_user_orders(self, user_id: str) -> List[Dict]:
        """Lấy danh sách đơn hàng của user - tự động sync isReviewed từ reviews collection"""
        try:
            from db.connection import reviews_collection
            
            orders = self.find_by_user_id(user_id)
            result = []
            
            for order in orders:
                order_dict = self._to_simple_response(order)
                
                # Sync isReviewed: kiểm tra xem order đã có review chưa
                if order.status.value == 'Completed' and not order.is_reviewed:
                    existing_review = reviews_collection.find_one({'orderId': order.id})
                    if existing_review:
                        # Cập nhật DB và response
                        self.collection.update_one(
                            {'_id': order.id},
                            {'$set': {'isReviewed': True}}
                        )
                        order_dict['isReviewed'] = True
                
                result.append(order_dict)
            
            return result
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy danh sách đơn: {str(e)}')

    def get_user_orders_by_status(self, user_id: str, status: str) -> List[Dict]:
        """Lấy danh sách đơn hàng của user theo trạng thái"""
        try:
            # Validate status
            valid_statuses = [s.value for s in OrderStatus]
            if status not in valid_statuses:
                raise ValueError(f'Trạng thái không hợp lệ. Các giá trị hợp lệ: {', '.join(valid_statuses)}')
            
            orders = []
            for doc in self.collection.find({'userId': ObjectId(user_id), 'status': status}).sort('createdAt', -1):
                orders.append(self._to_model(doc))
            
            return [self._to_simple_response(o) for o in orders]
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy đơn hàng theo trạng thái: {str(e)}')

    def get_restaurant_orders(self, restaurant_id: str) -> List[Dict]:
        """Lấy đơn hàng cho nhà hàng"""
        try:
            orders = self.find_by_restaurant_id(restaurant_id)
            return [self._to_simple_response(o) for o in orders]
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy đơn hàng nhà hàng: {str(e)}')

    def get_pending_orders(self, shipper_id: str) -> List[Dict]:
        """Lấy đơn hàng đang chờ (PENDING) - cho Shipper xem
        
        Chỉ trả về những đơn:
        - Status = PENDING
        - Chưa có shipperId (chưa có ai nhận) hoặc shipperId is null
        - Shipper này chưa từ chối (không có trong shipperRejections với shipperId này)
        """
        try:
            shipper_object_id = ObjectId(shipper_id)
            
            # Tìm tất cả đơn PENDING và chưa có shipper nhận
            # MongoDB: shipperId có thể là null hoặc không tồn tại
            query = {
                'status': OrderStatus.PENDING.value,
                '$or': [
                    {'shipperId': None},
                    {'shipperId': {'$exists': False}}
                ]
            }
            
            orders = []
            for doc in self.collection.find(query).sort('createdAt', -1):
                order = self._to_model(doc)
                
                # Kiểm tra shipper này đã từ chối đơn này chưa
                has_declined = False
                if order.shipper_rejections:
                    for rejection in order.shipper_rejections:
                        if rejection.shipper_id == shipper_object_id:
                            has_declined = True
                            break
                
                # Chỉ thêm vào danh sách nếu shipper này chưa từ chối
                if not has_declined:
                    orders.append(order)
            
            return [self._to_simple_response(o) for o in orders]
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy đơn chờ: {str(e)}')

    def get_shipper_orders(self, shipper_id: str) -> List[Dict]:
        """Lấy đơn hàng của shipper"""
        try:
            orders = self.find_by_shipper_id(shipper_id)
            return [self._to_simple_response(o) for o in orders]
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy đơn của shipper: {str(e)}')

    def get_all_orders(self) -> List[Dict]:
        """Lấy tất cả đơn (Admin only)"""
        try:
            orders = self.find_all()
            return [self._to_simple_response(o) for o in orders]
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy danh sách đơn: {str(e)}')

    def get_all_orders_by_status(self, status: str) -> List[Dict]:
        """Lấy tất cả đơn theo trạng thái (Admin only)"""
        try:
            # Validate status
            valid_statuses = [s.value for s in OrderStatus]
            if status not in valid_statuses:
                raise ValueError(f'Trạng thái không hợp lệ. Các giá trị hợp lệ: {', '.join(valid_statuses)}')
            
            orders = []
            for doc in self.collection.find({'status': status}).sort('createdAt', -1):
                orders.append(self._to_model(doc))
            
            return [self._to_simple_response(o) for o in orders]
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy đơn hàng theo trạng thái: {str(e)}')

    def accept_order(self, order_id: str, shipper_id: str) -> Dict:
        """Shipper nhận đơn: PENDING → SHIPPING"""
        try:
            order = self.find_by_id(order_id)
            if not order:
                raise ValueError('Không tìm thấy đơn hàng')
            
            if order.status != OrderStatus.PENDING:
                raise ValueError(f'Chỉ có thể nhận đơn ở trạng thái PENDING, hiện tại: {order.status.value}')
            
            # Cập nhật status và lưu thời gian nhận đơn
            now = datetime.now()
            result = self.collection.update_one(
                {'_id': ObjectId(order_id)},
                {
                    '$set': {
                        'status': OrderStatus.SHIPPING.value,
                        'shipperId': ObjectId(shipper_id),
                        'pickedAt': now,
                        'updatedAt': now
                    }
                }
            )
            
            if result.matched_count == 0:
                raise ValueError('Không thể cập nhật trạng thái')
            
            updated = self.find_by_id(order_id)
            return self._to_full_response(updated)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi nhận đơn: {str(e)}')

    def complete_order(self, order_id: str, shipper_id: str) -> Dict:
        """Shipper hoàn thành: SHIPPING → COMPLETED

        - Nếu đơn thanh toán bằng COD: khi hoàn thành sẽ tự động đánh dấu payment = Paid.
        """
        try:
            order = self.find_by_id(order_id)
            if not order:
                raise ValueError('Không tìm thấy đơn hàng')
            
            if order.status != OrderStatus.SHIPPING:
                raise ValueError('Chỉ có thể hoàn thành khi đơn đang SHIPPING')
            
            if str(order.shipper_id) != shipper_id:
                raise ValueError('Chỉ shipper nhận đơn mới có thể hoàn thành')
            
            updated = self.update_order_status_in_db(order_id, OrderStatus.COMPLETED.value)
            if not updated:
                raise ValueError('Không thể cập nhật trạng thái')

            # Nếu là COD thì auto đánh dấu payment đã thanh toán khi shipper hoàn thành
            try:
                payment = payment_service.find_by_order_id(order_id)
                if payment and payment.method == PaymentMethod.COD and payment.status == PaymentStatus.PENDING:
                    payment_service.mark_paid(str(payment.id))
            except Exception:
                # Không chặn luồng hoàn thành đơn nếu cập nhật payment gặp lỗi
                pass
            
            return self._to_full_response(updated)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi hoàn thành đơn: {str(e)}')

    def reject_order_by_shipper(self, order_id: str, shipper_id: str, reason: Optional[str] = None) -> Dict:
        """Shipper từ chối/hủy đơn đã nhận: SHIPPING → PENDING (bỏ shipperId)

        - Ghi lại lịch sử từ chối vào mảng shipperRejections: shipperId, reason, timestamp
        """
        try:
            order = self.find_by_id(order_id)
            if not order:
                raise ValueError('Không tìm thấy đơn hàng')
            
            if order.status != OrderStatus.SHIPPING:
                raise ValueError('Chỉ có thể từ chối đơn khi đang ở trạng thái SHIPPING')
            
            if str(order.shipper_id) != shipper_id:
                raise ValueError('Chỉ shipper nhận đơn mới có thể từ chối')
            
            # Reset về PENDING, xóa shipperId, xóa pickedAt và lưu lịch sử từ chối
            result = self.collection.update_one(
                {'_id': ObjectId(order_id)},
                {
                    '$set': {
                        'status': OrderStatus.PENDING.value,
                        'shipperId': None,
                        'pickedAt': None,  # Xóa thời gian nhận đơn của shipper cũ
                        'updatedAt': datetime.now()
                    },
                    '$push': {
                        'shipperRejections': {
                            'shipperId': ObjectId(shipper_id),
                            'reason': reason,
                            'timestamp': datetime.now()
                        }
                    }
                }
            )
            
            if result.matched_count == 0:
                raise ValueError('Không thể cập nhật trạng thái')
            
            updated = self.find_by_id(order_id)
            return self._to_full_response(updated)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi từ chối đơn: {str(e)}')

    def decline_pending_order(self, order_id: str, shipper_id: str, reason: Optional[str] = None) -> Dict:
        """Shipper từ chối đơn PENDING (chưa nhận)
        
        - Ghi lại lịch sử từ chối vào mảng shipperRejections
        - Đơn vẫn ở trạng thái PENDING (không thay đổi gì)
        """
        try:
            order = self.find_by_id(order_id)
            if not order:
                raise ValueError('Không tìm thấy đơn hàng')
            
            if order.status != OrderStatus.PENDING:
                raise ValueError('Chỉ có thể từ chối đơn khi đang ở trạng thái PENDING')
            
            # Thêm entry vào shipperRejections
            result = self.collection.update_one(
                {'_id': ObjectId(order_id)},
                {
                    '$push': {
                        'shipperRejections': {
                            'shipperId': ObjectId(shipper_id),
                            'reason': reason,
                            'timestamp': datetime.now()
                        }
                    },
                    '$set': {
                        'updatedAt': datetime.now()
                    }
                }
            )
            
            if result.matched_count == 0:
                raise ValueError('Không thể cập nhật đơn hàng')
            
            updated = self.find_by_id(order_id)
            return self._to_simple_response(updated)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi từ chối đơn: {str(e)}')

    def get_shipper_order_detail(self, order_id: str, shipper_id: str) -> Dict:
        """Shipper xem chi tiết đơn hàng của mình"""
        try:
            order = self.find_by_id(order_id)
            if not order:
                raise ValueError('Không tìm thấy đơn hàng')
            
            # Kiểm tra shipper có quyền xem đơn này không
            if order.shipper_id and str(order.shipper_id) != shipper_id:
                raise ValueError('Bạn không có quyền xem đơn hàng này')
            
            return self._to_full_response(order)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy chi tiết đơn: {str(e)}')

    def get_shipper_orders_by_status(self, shipper_id: str, status: Optional[str] = None) -> List[Dict]:
        """Lấy đơn hàng của shipper theo trạng thái (filter)"""
        try:
            query = {'shipperId': ObjectId(shipper_id)}
            if status:
                # Validate status
                valid_statuses = [s.value for s in OrderStatus]
                if status not in valid_statuses:
                    raise ValueError(f'Trạng thái không hợp lệ. Các giá trị hợp lệ: {", ".join(valid_statuses)}')
                query['status'] = status
            
            orders = []
            for doc in self.collection.find(query).sort('createdAt', -1):
                orders.append(self._to_model(doc))
            
            return [self._to_simple_response(o) for o in orders]
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy đơn hàng theo trạng thái: {str(e)}')

    def cancel_order(self, order_id: str, user_id: str, reason: Optional[str] = None) -> Dict:
        """User hủy đơn (chỉ khi PENDING)"""
        try:
            order = self.find_by_id(order_id)
            if not order:
                raise ValueError('Không tìm thấy đơn hàng')
            
            if order.status != OrderStatus.PENDING:
                raise ValueError('Chỉ có thể hủy đơn ở trạng thái PENDING')
            
            if str(order.user_id) != user_id:
                raise ValueError('Chỉ user đặt đơn mới có thể hủy')
            
            # Xử lý payment: refund nếu đã trả, hoặc đánh dấu thất bại nếu đang chờ
            if order.payment_id:
                payment = payment_service.find_by_order_id(order_id)
                if payment:
                    if payment.status == PaymentStatus.PAID:
                        payment_service.refund(str(order.payment_id))
                    elif payment.status == PaymentStatus.PENDING:
                        payment_service.mark_failed(str(order.payment_id))

            updated = self.cancel_order_in_db(order_id, 'user', reason)
            if not updated:
                raise ValueError('Không thể hủy đơn')

            # Hoàn lại voucher nếu đơn có sử dụng voucher
            try:
                if order.promo_id:
                    voucher_service.refund_voucher_used(str(order.promo_id), user_id)
            except Exception:
                # Không chặn flow nếu refund voucher lỗi
                pass
            
            return self._to_full_response(updated)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi hủy đơn: {str(e)}')

    def admin_cancel_order(self, order_id: str, reason: Optional[str] = None) -> Dict:
        """Admin hủy đơn (bất kỳ trạng thái nào)"""
        try:
            order = self.find_by_id(order_id)
            if not order:
                raise ValueError('Không tìm thấy đơn hàng')
            
            if order.status == OrderStatus.COMPLETED:
                raise ValueError('Không thể hủy đơn đã hoàn thành')
            
            if order.status == OrderStatus.CANCELLED:
                raise ValueError('Đơn hàng đã bị hủy rồi')
            
            # Xử lý payment: refund nếu đã trả, hoặc đánh dấu thất bại nếu đang chờ
            if order.payment_id:
                payment = payment_service.find_by_order_id(order_id)
                if payment:
                    if payment.status == PaymentStatus.PAID:
                        payment_service.refund(str(order.payment_id))
                    elif payment.status == PaymentStatus.PENDING:
                        payment_service.mark_failed(str(order.payment_id))

            updated = self.cancel_order_in_db(order_id, 'admin', reason)
            if not updated:
                raise ValueError('Không thể hủy đơn')

            # Hoàn lại voucher nếu đơn có sử dụng voucher
            try:
                if order.promo_id:
                    voucher_service.refund_voucher_used(str(order.promo_id), str(order.user_id))
            except Exception:
                # Không chặn flow nếu refund voucher lỗi
                pass
            
            return self._to_full_response(updated)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi hủy đơn: {str(e)}')

    def mark_paid(self, order_id: str) -> Dict:
        """Cập nhật trạng thái thanh toán thành PAID"""
        try:
            order = self.find_by_id(order_id)
            if not order:
                raise ValueError('Không tìm thấy đơn hàng')

            payment = payment_service.find_by_order_id(order_id)
            if not payment:
                raise ValueError('Không tìm thấy payment cho đơn hàng')

            payment_service.mark_paid(str(payment.id))

            # Trả về order (không thay đổi trạng thái đơn)
            return self._to_full_response(order)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f'Lỗi khi cập nhật trạng thái thanh toán: {str(e)}')


order_service = OrderService()
