from typing import Optional, Dict, List
from datetime import datetime
from bson import ObjectId
from pymongo.collection import Collection

from db.connection import cart_collection
from db.models.cart import Cart, CartItem
from schemas.cart_schema import (
    AddToCartRequest,
    UpdateCartItemRequest,
    CheckoutRequest,
    CartItemResponse,
    CartResponse
)


class CartService:
    def __init__(self, restaurant_service=None):
        self.collection: Collection = cart_collection
        # Import here to avoid circular dependency
        if restaurant_service is None:
            from services.restaurant_service import restaurant_service as rs
            self.restaurant_service = rs
        else:
            self.restaurant_service = rs

    # ==================== LAYER 1: MongoDB Operations ====================

    def _find_cart_by_user_id(self, user_id: str) -> Optional[Cart]:
        """Tìm giỏ hàng theo user_id"""
        try:
            doc = self.collection.find_one({"userId": ObjectId(user_id)})
            return Cart(**doc) if doc else None
        except Exception as e:
            print(f"Error finding cart: {e}")
            return None

    def _create_cart(self, user_id: str) -> Cart:
        """Tạo giỏ hàng mới cho user"""
        cart = Cart(userId=ObjectId(user_id), items=[])
        result = self.collection.insert_one(cart.to_mongo())
        cart.cart_id = result.inserted_id
        return cart

    def _update_cart(self, cart: Cart) -> bool:
        """Cập nhật giỏ hàng"""
        try:
            cart.updated_at = datetime.now()
            result = self.collection.update_one(
                {"_id": cart.cart_id},
                {"$set": cart.to_mongo()}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating cart: {e}")
            return False

    def _delete_cart(self, cart_id: ObjectId) -> bool:
        """Xóa giỏ hàng"""
        try:
            result = self.collection.delete_one({"_id": cart_id})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting cart: {e}")
            return False

    # ==================== LAYER 2: Business Logic ====================

    def _to_cart_response(self, cart: Cart) -> Dict:
        """Convert Cart model to response dict"""
        total_items = sum(item.quantity for item in cart.items)
        total_amount = sum(item.quantity * item.unit_price for item in cart.items)
        
        items_response = [
            CartItemResponse(
                restaurantId=str(item.restaurant_id),
                restaurantName=item.restaurant_name,
                foodName=item.food_name,
                quantity=item.quantity,
                unitPrice=item.unit_price,
                subtotal=item.quantity * item.unit_price
            ).model_dump(by_alias=True)
            for item in cart.items
        ]
        
        return CartResponse(
            _id=str(cart.cart_id),
            userId=str(cart.user_id),
            items=items_response,
            totalItems=total_items,
            totalAmount=total_amount,
            createdAt=cart.created_at.isoformat(),
            updatedAt=cart.updated_at.isoformat()
        ).model_dump(by_alias=True)

    def get_cart(self, user_id: str) -> Dict:
        """Lấy giỏ hàng của user (tạo mới nếu chưa có)"""
        try:
            cart = self._find_cart_by_user_id(user_id)
            if not cart:
                cart = self._create_cart(user_id)
            return self._to_cart_response(cart)
        except Exception as e:
            raise ValueError(f"Lỗi khi lấy giỏ hàng: {str(e)}")

    def add_to_cart(self, user_id: str, req: AddToCartRequest) -> Dict:
        """Thêm món vào giỏ"""
        try:
            # Lấy thông tin món ăn từ restaurant
            restaurant = self.restaurant_service.find_by_id(req.restaurant_id)
            if not restaurant:
                raise ValueError("Không tìm thấy nhà hàng")
            
            # Tìm món trong menu
            food_found = None
            for category in restaurant.menu:
                for food in category.items:
                    if food.name == req.food_name:
                        food_found = food
                        break
                if food_found:
                    break
            
            if not food_found:
                raise ValueError(f"Không tìm thấy món '{req.food_name}' trong menu")
            
            if not food_found.status:
                raise ValueError(f"Món '{req.food_name}' hiện không khả dụng")
            
            # Lấy hoặc tạo giỏ hàng
            cart = self._find_cart_by_user_id(user_id)
            if not cart:
                cart = self._create_cart(user_id)
            
            # Cho phép giỏ hàng chứa món từ nhiều nhà hàng (vì giỏ hàng = món yêu thích)
            # Kiểm tra món đã có trong giỏ chưa
            existing_item = None
            for item in cart.items:
                if item.food_name == req.food_name:
                    existing_item = item
                    break
            
            if existing_item:
                # Cập nhật số lượng
                existing_item.quantity += req.quantity
            else:
                # Thêm món mới
                new_item = CartItem(
                    restaurantId=ObjectId(req.restaurant_id),
                    restaurantName=restaurant.restaurant_name,
                    foodName=req.food_name,
                    quantity=req.quantity,
                    unitPrice=food_found.price
                )
                cart.items.append(new_item)
            
            # Lưu vào DB
            self._update_cart(cart)
            
            return self._to_cart_response(cart)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Lỗi khi thêm vào giỏ hàng: {str(e)}")

    def update_cart_item(self, user_id: str, food_name: str, req: UpdateCartItemRequest) -> Dict:
        """Cập nhật số lượng món trong giỏ"""
        try:
            cart = self._find_cart_by_user_id(user_id)
            if not cart:
                raise ValueError("Giỏ hàng trống")
            
            # Tìm món trong giỏ
            item_index = None
            for i, item in enumerate(cart.items):
                if item.food_name == food_name:
                    item_index = i
                    break
            
            if item_index is None:
                raise ValueError(f"Không tìm thấy món '{food_name}' trong giỏ hàng")
            
            if req.quantity == 0:
                # Xóa món
                cart.items.pop(item_index)
            else:
                # Cập nhật số lượng
                cart.items[item_index].quantity = req.quantity
            
            # Lưu vào DB
            self._update_cart(cart)
            
            return self._to_cart_response(cart)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Lỗi khi cập nhật giỏ hàng: {str(e)}")

    def remove_from_cart(self, user_id: str, food_name: str) -> Dict:
        """Xóa món khỏi giỏ"""
        try:
            cart = self._find_cart_by_user_id(user_id)
            if not cart:
                raise ValueError("Giỏ hàng trống")
            
            # Tìm và xóa món
            original_length = len(cart.items)
            cart.items = [item for item in cart.items if item.food_name != food_name]
            
            if len(cart.items) == original_length:
                raise ValueError(f"Không tìm thấy món '{food_name}' trong giỏ hàng")
            
            # Lưu vào DB
            self._update_cart(cart)
            
            return self._to_cart_response(cart)
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Lỗi khi xóa món khỏi giỏ: {str(e)}")

    def clear_cart(self, user_id: str) -> Dict:
        """Xóa toàn bộ giỏ hàng"""
        try:
            cart = self._find_cart_by_user_id(user_id)
            if not cart:
                raise ValueError("Giỏ hàng đã trống")
            
            # Xóa tất cả items
            cart.items = []
            self._update_cart(cart)
            
            return {"message": "Đã xóa toàn bộ giỏ hàng"}
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Lỗi khi xóa giỏ hàng: {str(e)}")

    def checkout_cart(self, user_id: str, req: CheckoutRequest) -> Dict:
        """Chuyển giỏ hàng thành đơn hàng"""
        try:
            # Import order_service here to avoid circular dependency
            from services.order_service import order_service
            from schemas.order_schema import CreateOrderRequest, CreateOrderItemRequest
            from db.models.payment import PaymentMethod
            
            cart = self._find_cart_by_user_id(user_id)
            if not cart or not cart.items:
                raise ValueError("Giỏ hàng trống, không thể thanh toán")
            
            # Chuyển đổi cart items thành order items
            order_items = [
                CreateOrderItemRequest(
                    food_name=item.food_name,
                    quantity=item.quantity
                )
                for item in cart.items
            ]
            
            # Tạo order request
            order_request = CreateOrderRequest(
                restaurantId=str(cart.items[0].restaurant_id),
                items=order_items,
                address=req.address,
                note=req.note,
                payment_method=PaymentMethod(req.payment_method),
                shipping_fee=req.shipping_fee,
                promoId=req.promo_id
            )
            
            # Tạo đơn hàng
            order = order_service.create_order(order_request, user_id)
            
            # Xóa giỏ hàng sau khi đặt hàng thành công
            cart.items = []
            self._update_cart(cart)
            
            return order
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Lỗi khi thanh toán: {str(e)}")


cart_service = CartService()
