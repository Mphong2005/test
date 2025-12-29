from typing import Optional, List, Dict
import re
import random
from bson import ObjectId
from pymongo.collection import Collection
from db.connection import restaurants_collection, reviews_collection, vouchers_collection
from db.models.restaurants import Restaurant, MenuCategory, FoodMenuItem
from schemas.restaurant_schema import (
    CreateRestaurantRequest,
    UpdateRestaurantRequest,
    RestaurantResponse,
    RestaurantSimpleResponse,
    SearchFoodResponse,
)
from utils.mongo_parser import parse_mongo_document

class RestaurantService:
    def __init__(self):
        self.collection: Collection = restaurants_collection

    # ==================== Helpers ====================
    def _to_model(self, doc: dict) -> Restaurant:
        """Convert MongoDB document to Restaurant model"""
        try:
            # Parse MongoDB Extended JSON format
            doc = parse_mongo_document(doc)
            
            # Ensure menu is properly structured
            if doc.get('menu') and isinstance(doc['menu'], list):
                # Menu should be list of categories with items
                for category in doc['menu']:
                    if isinstance(category, dict):
                        # Ensure items is a list
                        if 'items' in category and not isinstance(category['items'], list):
                            category['items'] = []
            return Restaurant(**doc)
        except Exception as e:
            print(f"Error converting document to Restaurant model: {e}")
            print(f"  Document keys: {list(doc.keys())}")
            print(f"  Document _id: {doc.get('_id')}")
            print(f"  Menu type: {type(doc.get('menu'))}")
            if doc.get('menu'):
                print(f"  Menu length: {len(doc.get('menu', []))}")
            raise

    def _to_simple_response(self, restaurant: Restaurant) -> Dict:
        """Convert Restaurant to simple response dict, calculating rating from reviews"""
        # Calculate rating from reviews
        try:
            restaurant_id = restaurant.restaurant_id
            reviews = list(reviews_collection.find({'restaurantId': restaurant_id}))
            total_reviews = len(reviews)
            if total_reviews > 0:
                avg_rating = sum(r.get('rating', 0) for r in reviews) / total_reviews
                avg_rating = round(avg_rating, 1)  # Round to 1 decimal place
            else:
                avg_rating = 0.0
        except Exception as e:
            print(f"Error calculating rating for restaurant {restaurant.restaurant_id}: {e}")
            avg_rating = round(restaurant.average_rating or 0.0, 1) if restaurant.average_rating else 0.0
            total_reviews = restaurant.total_reviews or 0
        
        # Update restaurant with calculated values
        restaurant.average_rating = avg_rating
        restaurant.total_reviews = total_reviews
        
        return RestaurantSimpleResponse(**restaurant.to_dict()).model_dump(by_alias=True)

    def _to_full_response(self, restaurant: Restaurant) -> Dict:
        """Convert Restaurant to full response dict with menu, calculating rating from reviews"""
        # Calculate rating from reviews
        try:
            restaurant_id = restaurant.restaurant_id
            reviews = list(reviews_collection.find({'restaurantId': restaurant_id}))
            total_reviews = len(reviews)
            if total_reviews > 0:
                avg_rating = sum(r.get('rating', 0) for r in reviews) / total_reviews
                avg_rating = round(avg_rating, 1)  # Round to 1 decimal place
            else:
                avg_rating = 0.0
        except Exception as e:
            print(f"Error calculating rating for restaurant {restaurant.restaurant_id}: {e}")
            avg_rating = round(restaurant.average_rating or 0.0, 1) if restaurant.average_rating else 0.0
            total_reviews = restaurant.total_reviews or 0
        
        # Update restaurant with calculated values
        restaurant.average_rating = avg_rating
        restaurant.total_reviews = total_reviews
        
        # Ensure menu is properly structured
        if not restaurant.menu:
            restaurant.menu = []
        
        # Convert to dict and ensure menu is properly serialized
        restaurant_dict = restaurant.to_dict()
        # Ensure menu is a list of dicts (not Pydantic models)
        if restaurant_dict.get('menu'):
            restaurant_dict['menu'] = [cat.to_dict() if hasattr(cat, 'to_dict') else cat for cat in restaurant.menu]
        
        return RestaurantResponse(**restaurant_dict).model_dump(by_alias=True)

    # ==================== LAYER 1: MongoDB CRUD Operations (Data Access) ====================

    def find_by_id(self, restaurant_id: str) -> Optional[Restaurant]:
        """Tìm nhà hàng theo ID - Trả về Model"""
        try:
            doc = self.collection.find_one({'_id': ObjectId(restaurant_id)})
            return self._to_model(doc) if doc else None
        except Exception as e:
            print(f"Error finding restaurant by id: {e}")
            return None

    def find_all(self) -> List[Restaurant]:
        """Lấy tất cả nhà hàng - Trả về List Model"""
        try:
            result = []
            for doc in self.collection.find():
                result.append(self._to_model(doc))
            return result
        except Exception as e:
            print(f"Error finding all restaurants: {e}")
            return []

    def create_restaurant_in_db(self, req: CreateRestaurantRequest) -> Optional[Restaurant]:
        """Tạo document mới - Input là Schema (để lấy data), Output là Model"""
        try:
            # Chuẩn hóa menu
            menu_list: List[MenuCategory] = []
            for cat in req.menu or []:
                if isinstance(cat, dict):
                    menu_list.append(MenuCategory(**cat))
                else:
                    menu_list.append(cat)
            
            restaurant = Restaurant(
                restaurant_name=req.restaurant_name,
                email=req.email,
                address=req.address,
                hotline=req.hotline,
                open_time=req.open_time,
                close_time=req.close_time,
                map_link=req.map_link,
                status=req.status if req.status is not None else True,
                menu=menu_list
            )
            insert_result = self.collection.insert_one(restaurant.to_mongo())
            return self.find_by_id(str(insert_result.inserted_id))
        except Exception as e:
            print(f"Error creating restaurant: {e}")
            # Lớp data nên raise error để lớp business biết
            raise ValueError(f'Lỗi DB khi tạo nhà hàng: {str(e)}')

    def update_restaurant_in_db(self, restaurant_id: str, req: UpdateRestaurantRequest) -> Optional[Restaurant]:
        """Update document - Output là Model"""
        try:
            update_data = req.model_dump(by_alias=True, exclude_none=True)
            if not update_data:
                raise ValueError('Không có dữ liệu để cập nhật')
            
            result = self.collection.update_one(
                {'_id': ObjectId(restaurant_id)},
                {'$set': update_data}
            )
            return self.find_by_id(restaurant_id) if result.matched_count > 0 else None
        except Exception as e:
            raise ValueError(f'Lỗi DB khi cập nhật nhà hàng: {str(e)}')

    def delete_restaurant_from_db(self, restaurant_id: str) -> bool:
        """Xóa document - Output là Bool"""
        try:
            result = self.collection.delete_one({'_id': ObjectId(restaurant_id)})
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting restaurant: {e}")
            return False

    def search_foods_by_name_and_category(self, query: str) -> List[Dict]:
        """Tìm món ăn (Aggregation phức tạp) - CHỈ TÌM Ở QUÁN ĐANG HOẠT ĐỘNG - Trả về List Dict (đã format sẵn để dùng)"""
        results: List[Dict] = []
        
        # 1. Pipeline Search món ăn
        pipeline_food = [
            {'$match': {'status': True}},  # CHỈ LẤY QUÁN ĐANG HOẠT ĐỘNG
            {'$unwind': {'path': '$menu', 'preserveNullAndEmptyArrays': False}},
            {'$unwind': {'path': '$menu.items', 'preserveNullAndEmptyArrays': False}},
            {'$match': {'menu.items.name': {'$regex': query, '$options': 'i'}}},
            {
                '$project': {
                    '_id': 1, 'name': 1, 'address': 1, 'hotline': 1, 
                    'openTime': 1, 'closeTime': 1, 'mapLink': 1,
                    'average_rating': 1,  # Thêm để hiển thị rating
                    'total_reviews': 1,   # Thêm để hiển thị số đánh giá
                    'category': '$menu.category', 'food': '$menu.items'
                }
            }
        ]
        food_results = list(self.collection.aggregate(pipeline_food))
        
        # Helper function để format kết quả search
        def _format_item(item):
            food_data = FoodMenuItem(**{
                'name': item['food'].get('name'),
                'price': float(item['food'].get('price', 0)),
                'description': item['food'].get('description'),
                'image': item['food'].get('image'),
                'status': item['food'].get('status', True)
            })
            restaurant_simple = RestaurantSimpleResponse(**{
                '_id': str(item['_id']),
                'name': item.get('name'),
                'address': item.get('address'),
                'hotline': item.get('hotline'),
                'openTime': item.get('openTime'),
                'closeTime': item.get('closeTime'),
                'mapLink': item.get('mapLink'),
                'averageRating': item.get('average_rating', 0.0),
                'totalReviews': item.get('total_reviews', 0),
            })
            return SearchFoodResponse(
                food=food_data,
                category=item.get('category'),
                restaurant=restaurant_simple
            ).model_dump()

        for item in food_results:
            results.append(_format_item(item))
        
        # 2. Pipeline Search Category
        pipeline_category = [
            {'$match': {'status': True}},  # CHỈ LẤY QUÁN ĐANG HOẠT ĐỘNG
            {'$unwind': {'path': '$menu', 'preserveNullAndEmptyArrays': False}},
            {'$match': {'menu.category': {'$regex': query, '$options': 'i'}}},
            {'$unwind': {'path': '$menu.items', 'preserveNullAndEmptyArrays': False}},
            {
                '$project': {
                    '_id': 1, 'name': 1, 'address': 1, 'hotline': 1, 
                    'openTime': 1, 'closeTime': 1, 'mapLink': 1,
                    'average_rating': 1,  # Thêm để hiển thị rating
                    'total_reviews': 1,   # Thêm để hiển thị số đánh giá
                    'category': '$menu.category', 'food': '$menu.items'
                }
            }
        ]
        category_results = list(self.collection.aggregate(pipeline_category))
        
        # Tránh trùng lặp
        added_keys = set(f"{item['_id']}_{item['category']}_{item['food']['name']}" for item in food_results)
        
        for item in category_results:
            key = f"{item['_id']}_{item['category']}_{item['food']['name']}"
            if key not in added_keys:
                results.append(_format_item(item))
                added_keys.add(key)
        
        return results

    def search_restaurants_by_name(self, query: str) -> List[Dict]:
        """Tìm nhà hàng theo tên (CHỈ QUÁN ĐANG HOẠT ĐỘNG) - Trả về List Simple Response Dict"""
        restaurants_cursor = self.collection.find({
            'name': {'$regex': query, '$options': 'i'},
            'status': True  # Chỉ tìm quán đang hoạt động
        })
        return [self._to_simple_response(self._to_model(doc)) for doc in restaurants_cursor]

    def admin_search_restaurants_by_name(self, query: str) -> List[Dict]:
        """Admin tìm nhà hàng theo tên (TẤT CẢ) - Trả về List Simple Response Dict"""
        restaurants_cursor = self.collection.find({'name': {'$regex': query, '$options': 'i'}})
        return [self._to_simple_response(self._to_model(doc)) for doc in restaurants_cursor]

    def _exists_by_name_address(self, name: Optional[str], address: Optional[str], exclude_id: Optional[str] = None) -> bool:
        """Kiểm tra nhà hàng trùng theo (name + address) - không phân biệt hoa thường"""
        name = (name or '').strip()
        address = (address or '').strip() if address else None
        if not name:
            return False
        query: Dict = {
            'name': {'$regex': f'^{re.escape(name)}$', '$options': 'i'}
        }
        if address:
            query['address'] = {'$regex': f'^{re.escape(address)}$', '$options': 'i'}
        if exclude_id:
            query['_id'] = {'$ne': ObjectId(exclude_id)}
        return self.collection.find_one(query) is not None

    # ==================== LAYER 2: Business Logic (Service Layer) ====================

    def get_all_restaurants(self) -> List[Dict]:
        """Lấy danh sách nhà hàng đang hoạt động (cho User)"""
        try:
            # Chỉ lấy nhà hàng có status: True
            restaurants_cursor = self.collection.find({'status': True})
            return [self._to_simple_response(self._to_model(doc)) for doc in restaurants_cursor]
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy danh sách nhà hàng: {str(e)}')

    def admin_get_all_restaurants(self) -> List[Dict]:
        """Lấy TẤT CẢ nhà hàng (bao gồm cả bị khóa) - CHỈ ADMIN"""
        try:
            restaurants = self.find_all()  # Lấy tất cả không filter
            return [self._to_simple_response(r) for r in restaurants]
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy danh sách nhà hàng: {str(e)}')

    def get_restaurant_by_id(self, restaurant_id: str) -> Dict:
        """Lấy thông tin nhà hàng đầy đủ theo ID (CHỈ NẾU ĐANG HOẠT ĐỘNG)"""
        restaurant = self.find_by_id(restaurant_id)
        if not restaurant:
            raise ValueError('Không tìm thấy nhà hàng')
        # Kiểm tra status - User không thể xem quán bị khóa
        if not restaurant.status:
            raise ValueError('Nhà hàng này hiện không hoạt động')
        return self._to_full_response(restaurant)

    def admin_get_restaurant_by_id(self, restaurant_id: str) -> Dict:
        """Admin lấy thông tin nhà hàng (kể cả bị khóa)"""
        restaurant = self.find_by_id(restaurant_id)
        if not restaurant:
            raise ValueError('Không tìm thấy nhà hàng')
        response = self._to_full_response(restaurant)
        # Debug: log menu structure
        print(f"Restaurant {restaurant_id} menu: {len(restaurant.menu) if restaurant.menu else 0} categories")
        if restaurant.menu:
            total_items = sum(len(cat.items) for cat in restaurant.menu)
            print(f"Total menu items: {total_items}")
        return response

    def create_restaurant(self, req: CreateRestaurantRequest) -> Dict:
        """Tạo nhà hàng mới - Kiểm tra trùng theo (name + address)"""
        # Kiểm tra trùng (name + address)
        if self._exists_by_name_address(req.restaurant_name, req.address):
            raise ValueError('Nhà hàng đã tồn tại (cùng tên và địa chỉ)')
        created = self.create_restaurant_in_db(req)
        if not created:
            raise ValueError('Không thể tạo nhà hàng')
        return self._to_full_response(created)

    def update_restaurant(self, restaurant_id: str, req: UpdateRestaurantRequest) -> Dict:
        """Cập nhật nhà hàng - Kiểm tra trùng theo (name + address) nếu có thay đổi"""
        # Lấy dữ liệu hiện tại
        current = self.find_by_id(restaurant_id)
        if not current:
            raise ValueError('Không tìm thấy nhà hàng')
        
        # Xác định giá trị sau cập nhật
        new_name = req.restaurant_name if req.restaurant_name is not None else current.restaurant_name
        new_address = req.address if req.address is not None else current.address
        
        # Kiểm tra nếu tên hoặc địa chỉ thay đổi
        name_changed = (new_name or '').strip().lower() != (current.restaurant_name or '').strip().lower()
        addr_changed = (new_address or '').strip().lower() != (current.address or '').strip().lower()
        
        if name_changed or addr_changed:
            # Nếu có thay đổi → kiểm tra trùng (loại trừ chính bản ghi)
            if self._exists_by_name_address(new_name, new_address, exclude_id=restaurant_id):
                raise ValueError('Nhà hàng đã tồn tại (cùng tên và địa chỉ)')
        
        updated = self.update_restaurant_in_db(restaurant_id, req)
        if not updated:
            raise ValueError('Không thể cập nhật nhà hàng')
        
        return self._to_full_response(updated)

    def delete_restaurant(self, restaurant_id: str) -> Dict:
        """Xóa nhà hàng"""
        if not self.find_by_id(restaurant_id):
            raise ValueError('Không tìm thấy nhà hàng')
        
        deleted = self.delete_restaurant_from_db(restaurant_id) # Gọi CRUD
        if not deleted:
            raise ValueError('Không thể xóa nhà hàng')
        
        return {'message': 'Xóa nhà hàng thành công'}

    def search_for_users(self, query: str) -> List[Dict]:
        """Search cho USER: Chỉ cần gọi CRUD food search"""
        try:
            return self.search_foods_by_name_and_category(query)
        except Exception as e:
            raise ValueError(f'Lỗi khi tìm kiếm: {str(e)}')
    
    def search_for_admin(self, query: str) -> Dict:
        """Search cho ADMIN: Kết hợp 2 hàm CRUD khác nhau (TẤT CẢ NHÀ HÀNG)"""
        try:
            # Logic: Admin cần cả 2 luồng dữ liệu riêng biệt
            result = {
                'restaurants': self.admin_search_restaurants_by_name(query),  # Lấy tất cả
                'foods': self.search_foods_by_name_and_category(query)  # Vẫn chỉ active (để test search)
            }
            return result
        except Exception as e:
            raise ValueError(f'Lỗi khi tìm kiếm: {str(e)}')

    def get_food_price(self, restaurant_id: str, food_name: str) -> Optional[float]:
        """Lấy giá của một món ăn từ DB (dùng cho order creation - bảo mật)"""
        try:
            restaurant = self.find_by_id(restaurant_id)
            if not restaurant:
                raise ValueError(f'Không tìm thấy nhà hàng {restaurant_id}')
            
            # Tìm food item trong menu
            for category in restaurant.menu:
                for item in category.items:
                    if item.name.lower() == food_name.lower():
                        return float(item.price)
            
            raise ValueError(f'Không tìm thấy món ăn "{food_name}" trong nhà hàng')
        except Exception as e:
            raise ValueError(f'Lỗi khi lấy giá món: {str(e)}')

    def toggle_restaurant_status(self, restaurant_id: str, status: bool) -> Dict:
        """Admin kích hoạt/vô hiệu hóa nhà hàng"""
        # Kiểm tra nhà hàng tồn tại
        restaurant = self.find_by_id(restaurant_id)
        if not restaurant:
            raise ValueError('Không tìm thấy nhà hàng')
        
        # Cập nhật trạng thái
        result = self.collection.update_one(
            {'_id': ObjectId(restaurant_id)},
            {'$set': {'status': status}}
        )
        
        if result.matched_count == 0:
            raise ValueError('Không thể cập nhật trạng thái nhà hàng')
        
        updated_restaurant = self.find_by_id(restaurant_id)
        action = "kích hoạt" if status else "vô hiệu hóa"
        
        return {
            'message': f'Đã {action} nhà hàng thành công',
            'restaurant': self._to_simple_response(updated_restaurant)
        }

    def get_promotions(self, limit: int = 8) -> List[Dict]:
        """
        Lấy danh sách promotions từ restaurants có reviews tốt nhất
        Dựa trên reviews_data.json thông qua reviews_collection
        Nếu không có reviews, vẫn hiển thị restaurants với promotions mặc định
        """
        try:
            # Lấy tất cả restaurants đang hoạt động
            restaurants = self.find_all()
            active_restaurants = [r for r in restaurants if r.status]
            
            if not active_restaurants:
                return []
            
            # Lấy reviews cho mỗi restaurant và tính điểm
            promotions = []
            for restaurant in active_restaurants:
                if not restaurant.restaurant_id:
                    continue
                
                restaurant_id = restaurant.restaurant_id
                restaurant_id_str = str(restaurant_id)
                
                # Đếm số reviews và tính rating trung bình
                # restaurantId trong MongoDB là ObjectId, không phải string
                reviews = list(reviews_collection.find({'restaurantId': restaurant_id}))
                total_reviews = len(reviews)
                
                # Tính rating trung bình (mặc định 4.0 nếu không có reviews)
                if total_reviews > 0:
                    avg_rating = sum(r.get('rating', 0) for r in reviews) / total_reviews
                else:
                    avg_rating = 4.0  # Rating mặc định cho restaurants chưa có reviews
                    total_reviews = 0
                
                # Lấy món ăn đầu tiên từ menu làm foodId
                food_id = None
                food_name = None
                food_image = None
                if restaurant.menu and len(restaurant.menu) > 0:
                    first_category = restaurant.menu[0]
                    if first_category.items and len(first_category.items) > 0:
                        first_food = first_category.items[0]
                        food_id = f"{restaurant_id_str}-{first_food.name}" if first_food.name else None
                        food_name = first_food.name
                        food_image = first_food.image if hasattr(first_food, 'image') and first_food.image else None
                
                # Lấy danh sách vouchers đang active từ database
                active_vouchers = list(vouchers_collection.find({
                    'active': True,
                    'first_order_only': False  # Chỉ lấy voucher áp dụng chung
                }))
                
                # Tạo action dựa trên voucher thực
                if active_vouchers:
                    # Random một voucher từ danh sách
                    voucher = random.choice(active_vouchers)
                    voucher_type = voucher.get('type', 'Percent')
                    voucher_value = voucher.get('value', 10)
                    
                    if voucher_type == 'Freeship':
                        action = "Free Ship"
                    elif voucher_type == 'Fixed':
                        # Format: Giảm 30K
                        action = f"Giảm {int(voucher_value/1000)}K"
                    elif voucher_type == 'Percent':
                        # Format: Giảm 15%
                        action = f"Giảm {int(voucher_value)}%"
                    else:
                        action = "Ưu đãi"
                else:
                    # Fallback nếu không có vouchers
                    action = "Ưu đãi đặc biệt"
                
                # Lấy image từ food hoặc restaurant
                image_url = food_image
                if not image_url and hasattr(restaurant, 'map_link') and restaurant.map_link:
                    image_url = restaurant.map_link
                
                # Fallback images dựa trên tên nhà hàng
                if not image_url:
                    name_lower = restaurant.restaurant_name.lower()
                    if "kfc" in name_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764462793/Screenshot_2025-11-29_092812_fgyur2.png"
                    elif "mcdonald" in name_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764462710/Screenshot_2025-11-30_072620_ak9ylz.png"
                    elif "phúc long" in name_lower or "phuclong" in name_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764462712/Screenshot_2025-11-30_072914_omsuvg.png"
                    elif "sushi" in name_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764462283/Screenshot_2025-11-30_072347_zzyj7x.png"
                    elif "pizza" in name_lower:
                        image_url = "https://images.unsplash.com/photo-1513104890138-7c749659a591?q=80&w=800"
                    elif "bánh mì" in name_lower or "banh mi" in name_lower:
                        image_url = "https://images.unsplash.com/photo-1541529086526-db283c563270?q=80&w=800"
                    elif "phở" in name_lower or "pho" in name_lower:
                        image_url = "https://images.unsplash.com/photo-1582878826618-c05326eff935?q=80&w=800"
                    elif "coffee" in name_lower:
                        image_url = "https://images.unsplash.com/photo-1509042239860-f550ce710b93?q=80&w=800"
                    else:
                        image_url = "https://images.unsplash.com/photo-1513104890138-7c749659a591?q=80&w=800"
                
                promotions.append({
                    'id': restaurant_id_str,
                    'name': food_name or restaurant.restaurant_name,  # Tên món ăn (hoặc fallback tên nhà hàng)
                    'foodName': food_name,  # Tên món ăn
                    'restaurantName': restaurant.restaurant_name,  # Tên nhà hàng
                    'vendor': restaurant.restaurant_name,  # Vendor là tên nhà hàng
                    'image': image_url,
                    'action': action,
                    'foodId': food_id or restaurant_id_str,
                    'rating': round(avg_rating, 1),
                    'totalReviews': total_reviews
                })
            
            # Sắp xếp theo rating và số lượng reviews (restaurants có reviews được ưu tiên)
            promotions.sort(key=lambda x: (x['totalReviews'] > 0, x['rating'], x['totalReviews']), reverse=True)
            
            # Giới hạn số lượng
            return promotions[:limit]
            
        except Exception as e:
            print(f"Error getting promotions: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_categories(self) -> List[Dict]:
        """
        Lấy danh sách categories từ tất cả restaurants
        Mỗi category sẽ có một hình ảnh random từ items trong category đó
        Loại bỏ các categories trùng lặp bằng cách normalize tên
        """
        try:
            import random
            
            # Lấy tất cả restaurants đang hoạt động
            restaurants = self.find_all()
            active_restaurants = [r for r in restaurants if r.status]
            
            # Dictionary để lưu categories và danh sách images
            # Key là normalized name để tránh trùng lặp
            categories_dict: Dict[str, Dict] = {}
            
            def normalize_category_name(name: str) -> str:
                """Chuẩn hóa tên category để loại bỏ trùng lặp"""
                if not name:
                    return ""
                # Chuyển về lowercase và loại bỏ khoảng trắng thừa
                normalized = name.strip().lower()
                # Loại bỏ các ký tự đặc biệt và chuẩn hóa
                normalized = normalized.replace('  ', ' ')
                return normalized
            
            # Duyệt qua tất cả restaurants và thu thập categories
            for restaurant in active_restaurants:
                if not restaurant.menu:
                    continue
                
                for menu_category in restaurant.menu:
                    category_name = menu_category.category
                    if not category_name:
                        continue
                    
                    # Chuẩn hóa tên category để loại bỏ trùng lặp
                    normalized_name = normalize_category_name(category_name)
                    if not normalized_name:
                        continue
                    
                    # Sử dụng normalized name làm key, nhưng lưu original name để hiển thị
                    if normalized_name not in categories_dict:
                        categories_dict[normalized_name] = {
                            'original_name': category_name,
                            'images': []
                        }
                    
                    # Thu thập images từ items trong category
                    if menu_category.items:
                        for item in menu_category.items:
                            # Kiểm tra image từ FoodMenuItem
                            item_image = None
                            if isinstance(item, FoodMenuItem):
                                item_image = item.image
                            elif isinstance(item, dict):
                                item_image = item.get('image')
                            elif hasattr(item, 'image'):
                                item_image = item.image
                            
                            if item_image and item_image not in categories_dict[normalized_name]['images']:
                                # Chỉ thêm nếu chưa có trong list để tránh duplicate images
                                categories_dict[normalized_name]['images'].append(item_image)
            
            # Tạo danh sách categories với hình ảnh random
            categories = []
            seen_names = set()  # Track để đảm bảo không có duplicate
            
            for normalized_name, category_data in categories_dict.items():
                original_name = category_data['original_name']
                images = category_data['images']
                
                # Kiểm tra xem đã có category này chưa (theo original name)
                if original_name in seen_names:
                    continue
                
                seen_names.add(original_name)
                
                # Chọn một hình ảnh random từ danh sách
                image_url = random.choice(images) if images else None
                
                # Nếu không có hình ảnh, sử dụng fallback dựa trên tên category
                if not image_url:
                    category_lower = normalized_name
                    if "phở" in category_lower or "pho" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461746/Screenshot_2025-11-30_070845_fcckkb.png"
                    elif "bún" in category_lower or "bun" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461746/Screenshot_2025-11-30_070845_fcckkb.png"
                    elif "cơm" in category_lower or "com" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764460831/Screenshot_2025-11-30_070009_s8hzez.png"
                    elif "đồ uống" in category_lower or "drink" in category_lower or ("trà" in category_lower and "trà sữa" not in category_lower and "trà trái" not in category_lower):
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461746/Screenshot_2025-11-30_070905_sde2iv.png"
                    elif "bánh" in category_lower or "banh" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461746/Screenshot_2025-11-30_070813_ldmmy9.png"
                    elif "xôi" in category_lower or "xoi" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764460831/Screenshot_2025-11-30_070009_s8hzez.png"
                    elif "lẩu" in category_lower or "lau" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461746/Screenshot_2025-11-30_070845_fcckkb.png"
                    elif "nướng" in category_lower or "nuong" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461748/Screenshot_2025-11-30_070923_miqawt.png"
                    elif "xào" in category_lower or "xao" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461748/Screenshot_2025-11-30_070923_miqawt.png"
                    elif "kem" in category_lower:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461748/Screenshot_2025-11-30_071007_euepva.png"
                    else:
                        image_url = "https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461748/Screenshot_2025-11-30_070923_miqawt.png"
                
                # Tạo ID từ normalized name
                category_id = normalized_name.replace(' ', '-').replace('đ', 'd').replace('ộ', 'o').replace('ồ', 'o').replace('á', 'a').replace('à', 'a').replace('ả', 'a').replace('ã', 'a').replace('ạ', 'a')
                
                categories.append({
                    'id': category_id,
                    'name': original_name,  # Sử dụng original name để hiển thị
                    'image': image_url
                })
            
            # Sắp xếp theo tên để có thứ tự nhất quán
            categories.sort(key=lambda x: x['name'])
            
            return categories
            
        except Exception as e:
            print(f"Error getting categories: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_all_foods(self) -> List[Dict]:
        """
        Lấy tất cả foods từ tất cả restaurants đang hoạt động
        Format: [{id, name, price, description, imageUrl, category, restaurantId, rating, ...}]
        """
        try:
            foods = []
            restaurants = self.find_all()
            active_restaurants = [r for r in restaurants if r.status]
            
            for restaurant in active_restaurants:
                if not restaurant.restaurant_id or not restaurant.menu:
                    continue
                
                restaurant_id_str = str(restaurant.restaurant_id)
                
                # Lấy reviews để tính rating
                reviews = list(reviews_collection.find({'restaurantId': restaurant.restaurant_id}))
                
                for menu_category in restaurant.menu:
                    category_name = menu_category.category
                    if not menu_category.items:
                        continue
                    
                    for item in menu_category.items:
                        # Tạo unique ID cho food: restaurantId-foodName
                        food_id = f"{restaurant_id_str}-{item.name}" if item.name else None
                        if not food_id:
                            continue
                        
                        # Tính rating từ reviews (có thể lấy từ food name hoặc restaurant)
                        # Tạm thời dùng restaurant rating
                        avg_rating = sum(r.get('rating', 0) for r in reviews) / len(reviews) if reviews else 4.0
                        
                        foods.append({
                            'id': food_id,
                            'name': item.name,
                            'price': float(item.price) if item.price else 0.0,
                            'description': item.description or '',
                            'imageUrl': item.image or '',
                            'category': category_name,
                            'restaurantId': restaurant_id_str,
                            'restaurantName': restaurant.restaurant_name,
                            'rating': round(avg_rating, 1),
                            'distance': '1.5',  # Default
                            'deliveryTime': '15-20 phút',  # Default
                            'status': item.status if hasattr(item, 'status') else True
                        })
            
            return foods
            
        except Exception as e:
            print(f"Error getting all foods: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_food_by_id(self, food_id: str) -> Optional[Dict]:
        """
        Lấy food item theo ID
        Format food_id: "restaurantId-foodName"
        """
        try:
            # Parse food_id: format "restaurantId-foodName"
            parts = food_id.split('-', 1)
            if len(parts) != 2:
                return None
            
            restaurant_id_str, food_name = parts
            try:
                restaurant_id = ObjectId(restaurant_id_str)
            except:
                return None
            
            # Tìm restaurant
            restaurant = self.find_by_id(restaurant_id_str)
            if not restaurant or not restaurant.status or not restaurant.menu:
                return None
            
            # Tìm food trong menu
            for menu_category in restaurant.menu:
                if not menu_category.items:
                    continue
                
                for item in menu_category.items:
                    if item.name == food_name:
                        # Lấy reviews để tính rating
                        reviews = list(reviews_collection.find({'restaurantId': restaurant_id}))
                        avg_rating = sum(r.get('rating', 0) for r in reviews) / len(reviews) if reviews else 4.0
                        
                        return {
                            'id': food_id,
                            'name': item.name,
                            'price': float(item.price) if item.price else 0.0,
                            'description': item.description or '',
                            'imageUrl': item.image or '',
                            'category': menu_category.category,
                            'restaurantId': restaurant_id_str,
                            'restaurantName': restaurant.restaurant_name,
                            'rating': round(avg_rating, 1),
                            'distance': '1.5',
                            'deliveryTime': '15-20 phút',
                            'status': item.status if hasattr(item, 'status') else True
                        }
            
            return None
            
        except Exception as e:
            print(f"Error getting food by id: {e}")
            import traceback
            traceback.print_exc()
            return None
            
restaurant_service = RestaurantService()