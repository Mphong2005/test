from typing import Optional, List, Dict
from datetime import datetime
from bson import ObjectId

from db.connection import reviews_collection, orders_collection, restaurants_collection, users_collection
from db.models.review import Review
from db.models.order import OrderStatus
from utils.mongo_parser import parse_mongo_document


class ReviewService:
    """
    Review Service - Xử lý nghiệp vụ đánh giá nhà hàng
    
    Chức năng chính:
    - CRUD review (tạo, đọc, cập nhật, xóa)
    - Validate business rules (chỉ review đơn Completed, 1 review/order)
    - Tính toán rating trung bình cho nhà hàng
    - Lấy danh sách reviews theo user/restaurant
    """
    def __init__(self):
        self.collection = reviews_collection

    # ==================== Helpers ====================
    def _to_model(self, doc: dict) -> Review:
        """Chuyển MongoDB document thành Review model"""
        # Parse MongoDB Extended JSON format
        doc = parse_mongo_document(doc)
        return Review(**doc)

    def _to_dict(self, review: Review) -> Dict:
        """Chuyển Review model thành dict để trả về API"""
        return review.to_dict()

    # ==================== LAYER 1: MongoDB CRUD Operations ====================
    
    def create(self, order_id: str, user_id: str, rating: int, comment: Optional[str] = None) -> Dict:
        """
        Tạo review mới
        
        Validate:
        - Order phải tồn tại và thuộc về user
        - Order phải ở trạng thái Completed
        - Order chưa được review (check orderId unique)
        """
        # Kiểm tra order
        order = orders_collection.find_one({'_id': ObjectId(order_id)})
        if not order:
            raise ValueError('Không tìm thấy đơn hàng')
        
        if str(order['userId']) != user_id:
            raise ValueError('Đơn hàng này không thuộc về bạn')
        
        if order['status'] != OrderStatus.COMPLETED.value:
            raise ValueError('Chỉ có thể đánh giá đơn hàng đã hoàn thành')
        
        # Kiểm tra đã review chưa
        existing = self.collection.find_one({'orderId': ObjectId(order_id)})
        if existing:
            raise ValueError('Đơn hàng này đã được đánh giá rồi. Bạn có thể chỉnh sửa đánh giá.')
        
        # Tạo review
        review = Review(
            order_id=ObjectId(order_id),
            user_id=ObjectId(user_id),
            restaurant_id=order['restaurantId'],
            rating=rating,
            comment=comment,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        result = self.collection.insert_one(review.to_mongo())
        created = self.find_by_id(str(result.inserted_id))
        
        # Cập nhật order: đánh dấu đã review
        orders_collection.update_one(
            {'_id': ObjectId(order_id)},
            {'$set': {'isReviewed': True, 'updatedAt': datetime.now()}}
        )
        
        # Cập nhật rating nhà hàng
        self._update_restaurant_rating(str(order['restaurantId']))
        
        return self._to_dict(created)

    def update(self, review_id: str, user_id: str, rating: Optional[int] = None, comment: Optional[str] = None) -> Dict:
        """
        Cập nhật review
        
        Validate:
        - Review phải tồn tại và thuộc về user
        """
        review = self.find_by_id(review_id)
        if not review:
            raise ValueError('Không tìm thấy đánh giá')
        
        if str(review.user_id) != user_id:
            raise ValueError('Bạn không có quyền chỉnh sửa đánh giá này')
        
        updates = {'updatedAt': datetime.now()}
        if rating is not None:
            updates['rating'] = rating
        if comment is not None:
            updates['comment'] = comment
        
        self.collection.update_one({'_id': ObjectId(review_id)}, {'$set': updates})
        updated = self.find_by_id(review_id)
        
        # Cập nhật rating nhà hàng
        self._update_restaurant_rating(str(review.restaurant_id))
        
        return self._to_dict(updated)

    def delete(self, review_id: str, user_id: str) -> None:
        """
        Xóa review
        
        Validate:
        - Review phải tồn tại và thuộc về user
        """
        review = self.find_by_id(review_id)
        if not review:
            raise ValueError('Không tìm thấy đánh giá')
        
        if str(review.user_id) != user_id:
            raise ValueError('Bạn không có quyền xóa đánh giá này')
        
        restaurant_id = str(review.restaurant_id)
        order_id = review.order_id
        
        self.collection.delete_one({'_id': ObjectId(review_id)})
        
        # Cập nhật order: đánh dấu chưa review
        if order_id:
            orders_collection.update_one(
                {'_id': order_id},
                {'$set': {'isReviewed': False, 'updatedAt': datetime.now()}}
            )
        
        # Cập nhật rating nhà hàng
        self._update_restaurant_rating(restaurant_id)

    # ==================== LAYER 2: Business Logic ====================

    def find_by_id(self, review_id: str) -> Optional[Review]:
        """Tìm review theo ID"""
        doc = self.collection.find_one({'_id': ObjectId(review_id)})
        return self._to_model(doc) if doc else None

    def find_by_order_id(self, order_id: str) -> Optional[Review]:
        """Tìm review theo order ID"""
        doc = self.collection.find_one({'orderId': ObjectId(order_id)})
        return self._to_model(doc) if doc else None

    def find_by_user_id(self, user_id: str) -> List[Dict]:
        """Lấy tất cả reviews của user"""
        cursor = self.collection.find({'userId': ObjectId(user_id)}).sort('createdAt', -1)
        reviews = []
        for doc in cursor:
            review = self._to_model(doc)
            review_dict = self._to_dict(review)
            # Hydrate thông tin restaurant
            restaurant = restaurants_collection.find_one({'_id': review.restaurant_id})
            if restaurant:
                review_dict['restaurantName'] = restaurant.get('name')
            reviews.append(review_dict)
        return reviews

    def find_by_restaurant_id(self, restaurant_id: str) -> List[Dict]:
        """Lấy tất cả reviews của nhà hàng"""
        cursor = self.collection.find({'restaurantId': ObjectId(restaurant_id)}).sort('createdAt', -1)
        reviews = []
        for doc in cursor:
            review = self._to_model(doc)
            review_dict = self._to_dict(review)
            # Hydrate thông tin user
            user = users_collection.find_one({'_id': review.user_id})
            if user:
                review_dict['userFullname'] = user.get('fullname', 'Anonymous')
            reviews.append(review_dict)
        return reviews

    def find_by_food_id(self, food_id: str) -> List[Dict]:
        """
        Lấy tất cả reviews của một món ăn
        Format food_id: "restaurantId-foodName"
        """
        try:
            # Parse food_id: format "restaurantId-foodName"
            parts = food_id.split('-', 1)
            if len(parts) != 2:
                return []
            
            restaurant_id_str, food_name = parts
            try:
                restaurant_id = ObjectId(restaurant_id_str)
            except:
                return []
            
            # Lấy tất cả reviews của restaurant
            cursor = self.collection.find({'restaurantId': restaurant_id}).sort('createdAt', -1)
            reviews = []
            
            for doc in cursor:
                review = self._to_model(doc)
                review_dict = self._to_dict(review)
                
                # Kiểm tra xem review có foodName không, nếu có thì filter theo foodName
                # Nếu không có foodName trong review, thì lấy tất cả reviews của restaurant (fallback)
                if 'foodName' in doc and doc['foodName']:
                    if doc['foodName'] != food_name:
                        continue
                # Nếu không có foodName, vẫn lấy review (để hiển thị reviews của restaurant)
                
                # Hydrate thông tin user
                user = users_collection.find_one({'_id': review.user_id})
                if user:
                    review_dict['userFullname'] = user.get('fullname', 'Anonymous')
                    review_dict['userAvatar'] = user.get('avatar')
                
                reviews.append(review_dict)
            
            return reviews
        except Exception as e:
            print(f"Error finding reviews by food_id: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_restaurant_rating_stats(self, restaurant_id: str) -> Dict:
        """
        Tính toán thống kê rating cho nhà hàng
        - Average rating
        - Total reviews
        - Rating distribution (1-5 sao)
        """
        pipeline = [
            {'$match': {'restaurantId': ObjectId(restaurant_id)}},
            {
                '$group': {
                    '_id': None,
                    'averageRating': {'$avg': '$rating'},
                    'totalReviews': {'$sum': 1},
                    'ratings': {'$push': '$rating'}
                }
            }
        ]
        
        result = list(self.collection.aggregate(pipeline))
        
        if not result:
            return {
                'restaurantId': restaurant_id,
                'averageRating': 0.0,
                'totalReviews': 0,
                'ratingDistribution': {str(i): 0 for i in range(1, 6)}
            }
        
        data = result[0]
        ratings = data['ratings']
        
        # Tính phân bố rating
        distribution = {str(i): 0 for i in range(1, 6)}
        for r in ratings:
            distribution[str(r)] += 1
        
        return {
            'restaurantId': restaurant_id,
            'averageRating': round(data['averageRating'], 2),
            'totalReviews': data['totalReviews'],
            'ratingDistribution': distribution
        }

    def _update_restaurant_rating(self, restaurant_id: str) -> None:
        """
        Cập nhật rating trung bình vào restaurant document
        Gọi sau khi create/update/delete review
        Dùng snake_case cho MongoDB fields để đồng bộ với model
        """
        try:
            stats = self.get_restaurant_rating_stats(restaurant_id)
            restaurants_collection.update_one(
                {'_id': ObjectId(restaurant_id)},
                {
                    '$set': {
                        'average_rating': stats['averageRating'],  # snake_case trong DB
                        'total_reviews': stats['totalReviews']
                    }
                }
            )
        except Exception as e:
            print(f"Error updating restaurant rating: {e}")

    def check_order_reviewable(self, order_id: str, user_id: str) -> Dict:
        """
        Kiểm tra đơn hàng có thể đánh giá không
        
        Returns:
        - canReview: bool
        - reason: string (nếu không thể review)
        - existingReview: object (nếu đã review)
        """
        # Kiểm tra order
        order = orders_collection.find_one({'_id': ObjectId(order_id)})
        if not order:
            return {'canReview': False, 'reason': 'Không tìm thấy đơn hàng'}
        
        if str(order['userId']) != user_id:
            return {'canReview': False, 'reason': 'Đơn hàng không thuộc về bạn'}
        
        if order['status'] != OrderStatus.COMPLETED.value:
            return {'canReview': False, 'reason': 'Chỉ có thể đánh giá đơn hàng đã hoàn thành'}
        
        # Kiểm tra đã review chưa
        existing_review = self.find_by_order_id(order_id)
        if existing_review:
            return {
                'canReview': False,
                'reason': 'Đơn hàng đã được đánh giá',
                'existingReview': self._to_dict(existing_review)
            }
        
        return {'canReview': True}

    def get_reviewable_orders(self, user_id: str) -> List[Dict]:
        """
        Lấy danh sách đơn hàng của user ở trạng thái Completed và CHƯA có review
        Dùng cho màn hình "Đánh giá ngay"
        """
        # Lấy tất cả orders Completed của user
        orders_cursor = orders_collection.find({
            'userId': ObjectId(user_id),
            'status': OrderStatus.COMPLETED.value
        }).sort('createdAt', -1)

        result: List[Dict] = []
        for order_doc in orders_cursor:
            # Bỏ qua nếu đã có review
            existing = self.collection.find_one({'orderId': order_doc['_id']})
            if existing:
                continue

            # Build response item
            item = {
                'orderId': str(order_doc['_id']),
                'restaurantId': str(order_doc.get('restaurantId')) if order_doc.get('restaurantId') else None,
                'restaurantName': order_doc.get('restaurantName'),
                'totalAmount': float(order_doc.get('total_amount', 0)),
                'createdAt': order_doc.get('createdAt'),
                'status': order_doc.get('status')
            }

            # Thêm meta từ restaurant (rating) nếu cần
            if order_doc.get('restaurantId'):
                restaurant = restaurants_collection.find_one({'_id': order_doc.get('restaurantId')})
                if restaurant:
                    item['averageRating'] = float(restaurant.get('average_rating', 0.0))
                    item['totalReviews'] = int(restaurant.get('total_reviews', 0))

            result.append(item)

        return result


review_service = ReviewService()
