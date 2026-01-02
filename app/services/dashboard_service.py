from typing import List, Dict, Optional
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from pymongo.collection import Collection

from db.connection import orders_collection, users_collection, restaurants_collection, payments_collection
from db.models.order import OrderStatus
from db.models.payment import PaymentStatus
from schemas.dashboard_schema import (
    OverviewStats,
    MonthlyRevenueData,
    MonthlyRevenueResponse,
    OrderStatusDistribution,
    RecentActivity,
    TopSellingItem,
    TopRevenueRestaurant,
    DashboardFullResponse,
    ShipperOverviewStats,
    ShipperActivityHistory,
    ShipperMonthlyRevenueData,
    ShipperDashboardFullResponse
)


class DashboardService:
    def __init__(self):
        self.orders_collection: Collection = orders_collection
        self.users_collection: Collection = users_collection
        self.restaurants_collection: Collection = restaurants_collection
        self.payments_collection: Collection = payments_collection

    # ==================== LAYER 1: MongoDB Aggregation Operations ====================

    def _parse_datetime(self, dt) -> Optional[datetime]:
        """Helper để parse datetime từ nhiều format"""
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return dt
        if isinstance(dt, str):
            try:
                # Thử parse ISO format (2025-12-20T10:30:00.000+00:00)
                # datetime.fromisoformat() hỗ trợ format này từ Python 3.7+
                dt_clean = dt.replace('Z', '+00:00')
                # Parse và loại bỏ timezone info (chuyển về naive datetime)
                parsed = datetime.fromisoformat(dt_clean)
                if parsed.tzinfo:
                    # Convert về UTC và loại bỏ timezone info
                    parsed = parsed.replace(tzinfo=None) + timedelta(seconds=parsed.utcoffset().total_seconds() if parsed.utcoffset() else 0)
                return parsed
            except (ValueError, AttributeError, TypeError):
                try:
                    # Fallback: parse với format khác
                    return datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
                except:
                    try:
                        # Fallback khác: chỉ lấy phần date
                        return datetime.strptime(dt[:10], '%Y-%m-%d')
                    except:
                        return None
        return None

    def _aggregate_total_revenue(self, start_date: datetime, end_date: Optional[datetime] = None) -> float:
        """Tính tổng doanh thu trong khoảng thời gian"""
        # Query tất cả orders Completed và filter trong Python để xử lý cả datetime và string
        all_completed = list(self.orders_collection.find({"status": OrderStatus.COMPLETED.value}))
        total = 0.0
        
        for order in all_completed:
            created_at = self._parse_datetime(order.get("createdAt"))
            if created_at and created_at >= start_date:
                if end_date is None or created_at < end_date:
                    total += float(order.get("total_amount", 0))
        
        return total

    def _get_active_user_ids(self, start_date: datetime) -> List[ObjectId]:
        """Lấy danh sách user_id có hoạt động từ start_date (bao gồm cả user và shipper)"""
        # Lấy tất cả orders và filter trong Python để xử lý cả datetime và string
        all_orders = list(self.orders_collection.find({}, {"userId": 1, "shipperId": 1, "createdAt": 1}))
        user_ids = set()
        
        for order in all_orders:
            created_at = self._parse_datetime(order.get("createdAt"))
            if created_at and created_at >= start_date:
                # Thêm cả userId (người đặt hàng) và shipperId (người giao hàng)
                user_id = order.get("userId")
                if user_id:
                    try:
                        user_ids.add(user_id if isinstance(user_id, ObjectId) else ObjectId(user_id))
                    except:
                        pass
                
                shipper_id = order.get("shipperId")
                if shipper_id:
                    try:
                        user_ids.add(shipper_id if isinstance(shipper_id, ObjectId) else ObjectId(shipper_id))
                    except:
                        pass
        
        return list(user_ids)

    def _count_active_users(self) -> int:
        """Đếm số người dùng active (bao gồm cả user và shipper, loại trừ admin)"""
        # Đếm tất cả users và shippers có is_active = true
        return self.users_collection.count_documents({"is_active": True})

    def _count_active_restaurants(self) -> int:
        """Đếm số nhà hàng đang hoạt động"""
        return self.restaurants_collection.count_documents({"status": True})

    def _aggregate_revenue_by_month(self, year: int) -> Dict[int, float]:
        """Aggregate doanh thu theo từng tháng trong năm"""
        # Lấy tất cả orders Completed và filter trong Python
        all_completed = list(self.orders_collection.find({"status": OrderStatus.COMPLETED.value}))
        revenue_by_month = {i: 0.0 for i in range(1, 13)}
        start_of_year = datetime(year, 1, 1)
        end_of_year = datetime(year + 1, 1, 1)
        
        for order in all_completed:
            created_at = self._parse_datetime(order.get("createdAt"))
            if created_at and start_of_year <= created_at < end_of_year:
                month = created_at.month
                revenue_by_month[month] += float(order.get("total_amount", 0))
        
        return revenue_by_month

    def _aggregate_order_count_by_status(self) -> Dict[str, int]:
        """Đếm số đơn hàng theo từng trạng thái"""
        pipeline = [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
        ]
        results = list(self.orders_collection.aggregate(pipeline))
        return {r["_id"]: r["count"] for r in results}

    def _get_recent_orders_with_user(self, limit: int) -> List[Dict]:
        """Lấy danh sách đơn hàng gần đây kèm thông tin user"""
        pipeline = [
            {
                "$sort": {
                    "updatedAt": -1,  # Ưu tiên thời gian cập nhật mới nhất
                    "_id": -1         # Nếu bằng nhau, xếp theo _id (ObjectId chứa timestamp)
                }
            },
            {"$limit": limit},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "userId",
                    "foreignField": "_id",
                    "as": "user"
                }
            },
            {"$unwind": {"path": "$user", "preserveNullAndEmptyArrays": True}}
        ]
        return list(self.orders_collection.aggregate(pipeline))

    def _aggregate_top_selling_items(self, limit: int) -> List[Dict]:
        """Aggregate món ăn bán chạy nhất (phân biệt theo nhà hàng)"""
        # Lấy tất cả orders hợp lệ và xử lý trong Python để đảm bảo logic chính xác
        orders = list(self.orders_collection.find({
            "status": {"$in": [OrderStatus.COMPLETED.value, OrderStatus.SHIPPING.value]},
            "items": {"$exists": True, "$ne": []},
            "restaurantId": {"$exists": True, "$ne": None},
            "restaurantName": {"$exists": True, "$ne": None, "$ne": ""}
        }))
        
        # Dictionary để nhóm và đếm: key = (restaurantId, food_name)
        food_stats = {}
        
        for order in orders:
            restaurant_id = order.get("restaurantId")
            restaurant_name = order.get("restaurantName", "")
            items = order.get("items", [])
            
            if not restaurant_id or not restaurant_name or not items:
                continue
            
            for item in items:
                food_name = item.get("food_name")
                quantity = item.get("quantity", 0)
                
                # Kiểm tra item hợp lệ
                if not food_name or not isinstance(food_name, str) or len(food_name.strip()) == 0:
                    continue
                
                if not isinstance(quantity, (int, float)) or quantity <= 0:
                    continue
                
                # Tạo key duy nhất cho mỗi món ở mỗi nhà hàng
                key = (str(restaurant_id), food_name.strip())
                
                if key not in food_stats:
                    food_stats[key] = {
                        "foodName": food_name.strip(),
                        "restaurantName": restaurant_name,
                        "restaurantId": restaurant_id,
                        "totalSold": 0
                    }
                
                food_stats[key]["totalSold"] += int(quantity)
        
        # Chuyển sang list và sắp xếp
        results = list(food_stats.values())
        results.sort(key=lambda x: (-x["totalSold"], x["foodName"], x["restaurantName"]))
        
        # Giới hạn số lượng
        results = results[:limit]
        
        # Debug: Log kết quả để kiểm tra
        print(f"[DEBUG] Top selling items found: {len(results)}")
        for idx, item in enumerate(results[:10], 1):
            print(f"[DEBUG] {idx}. {item.get('foodName', 'N/A')} - Sold: {item.get('totalSold', 0)} - Restaurant: {item.get('restaurantName', 'N/A')}")
        
        return results

    def _aggregate_top_revenue_restaurants(self, limit: int) -> List[Dict]:
        """Aggregate nhà hàng có doanh thu cao nhất"""
        pipeline = [
            {"$match": {"status": OrderStatus.COMPLETED.value}},
            {
                "$group": {
                    "_id": "$restaurantId",
                    "totalRevenue": {"$sum": "$total_amount"},
                    "restaurantName": {"$first": "$restaurantName"}  # Lấy restaurantName để sử dụng cho sort
                }
            },
            {
                "$sort": {
                    "totalRevenue": -1,      # Ưu tiên doanh thu cao nhất
                    "restaurantName": 1      # Nếu bằng nhau, xếp theo tên A-Z
                }
            },
            {"$limit": limit},
            {
                "$lookup": {
                    "from": "restaurants",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "restaurant"
                }
            },
            {"$unwind": {"path": "$restaurant", "preserveNullAndEmptyArrays": True}}
        ]
        return list(self.orders_collection.aggregate(pipeline))

    # ==================== LAYER 2: Business Logic ====================

    def get_overview_stats(self) -> Dict:
        """Lấy thống kê tổng quan cho dashboard"""
        now = datetime.now()
        start_of_month = datetime(now.year, now.month, 1)
        start_of_today = datetime(now.year, now.month, now.day)

        # Sử dụng các hàm LAYER 1
        total_revenue_month = self._aggregate_total_revenue(start_of_month)
        revenue_today = self._aggregate_total_revenue(start_of_today)
        # Đếm tất cả users và shippers active trong hệ thống (không phải chỉ những người có đặt hàng)
        total_active_users = self._count_active_users()
        total_restaurants = self._count_active_restaurants()

        return OverviewStats(
            totalRevenueMonth=total_revenue_month,
            revenueToday=revenue_today,
            activeUsers=total_active_users,
            totalRestaurants=total_restaurants
        ).model_dump(by_alias=True)

    def get_monthly_revenue(self, year: Optional[int] = None) -> Dict:
        """Lấy doanh thu theo từng tháng trong năm"""
        if year is None:
            year = datetime.now().year

        # Lấy dữ liệu từ LAYER 1
        revenue_map = self._aggregate_revenue_by_month(year)
        
        # Tạo data cho tất cả 12 tháng (điền 0 nếu không có doanh thu)
        monthly_data = []
        for month in range(1, 13):
            monthly_data.append(
                MonthlyRevenueData(
                    month=f"T{month}",
                    revenue=revenue_map.get(month, 0.0)
                )
            )

        return MonthlyRevenueResponse(
            year=year,
            data=monthly_data
        ).model_dump(by_alias=True)

    def get_order_status_distribution(self) -> Dict:
        """Lấy phân bố trạng thái đơn hàng"""
        # Lấy dữ liệu từ LAYER 1
        status_map = self._aggregate_order_count_by_status()

        return OrderStatusDistribution(
            completed=status_map.get(OrderStatus.COMPLETED.value, 0),
            pending=status_map.get(OrderStatus.PENDING.value, 0),
            shipping=status_map.get(OrderStatus.SHIPPING.value, 0),
            cancelled=status_map.get(OrderStatus.CANCELLED.value, 0)
        ).model_dump(by_alias=True)

    def get_recent_activities(self, limit: int = 10) -> List[Dict]:
        """Lấy hoạt động gần đây"""
        # Lấy dữ liệu từ LAYER 1
        orders = self._get_recent_orders_with_user(limit)
        
        # Xử lý và format dữ liệu
        activities = []
        type_map = {
            OrderStatus.PENDING.value: "Đặt đơn mới",
            OrderStatus.SHIPPING.value: "Đang giao hàng",
            OrderStatus.COMPLETED.value: "Giao thành công",
            OrderStatus.CANCELLED.value: "Đơn bị hủy"
        }
        
        for order in orders:
            # Format timestamp với UTC timezone info để frontend có thể convert sang Vietnam time
            updated_at = order.get("updatedAt")
            if isinstance(updated_at, datetime):
                # Nếu datetime không có timezone info, giả định là UTC
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                timestamp_str = updated_at.isoformat()
            elif isinstance(updated_at, str):
                # Nếu là string và không có timezone info, thêm +00:00
                timestamp_str = updated_at if '+' in updated_at or 'Z' in updated_at else f"{updated_at}+00:00"
            else:
                timestamp_str = datetime.now(timezone.utc).isoformat()
            
            activities.append(
                RecentActivity(
                    orderId=f"#ORD-{str(order['_id'])[-4:]}",
                    userName=order.get("user", {}).get("fullname", "Unknown"),
                    status=order.get("status", "Unknown"),
                    timestamp=timestamp_str,
                    type=type_map.get(order.get("status"), "Unknown")
                ).model_dump(by_alias=True)
            )
        
        return activities

    def get_top_selling_items(self, limit: int = 10) -> List[Dict]:
        """Lấy top món ăn bán chạy nhất toàn sàn (phân biệt theo nhà hàng)"""
        # Lấy dữ liệu từ LAYER 1
        results = self._aggregate_top_selling_items(limit)
        
        # Format dữ liệu
        top_items = []
        for item in results:
            top_items.append(
                TopSellingItem(
                    foodName=item["foodName"],
                    restaurantName=item["restaurantName"],
                    restaurantId=f"#RES-{str(item['restaurantId'])[-3:]}",
                    totalSold=item["totalSold"]
                ).model_dump(by_alias=True)
            )
        
        return top_items

    def get_top_revenue_restaurants(self, limit: int = 10) -> List[Dict]:
        """Lấy top nhà hàng có doanh thu cao nhất"""
        # Lấy dữ liệu từ LAYER 1
        results = self._aggregate_top_revenue_restaurants(limit)
        
        # Format dữ liệu
        top_restaurants = []
        for item in results:
            restaurant = item.get("restaurant", {})
            top_restaurants.append(
                TopRevenueRestaurant(
                    restaurantId=f"#RES-{str(item['_id'])[-3:]}",
                    restaurantName=restaurant.get("name", "Unknown Restaurant"),
                    totalRevenue=float(item["totalRevenue"])
                ).model_dump(by_alias=True)
            )
        
        return top_restaurants

    def get_full_dashboard_data(self) -> Dict:
        """Lấy tất cả dữ liệu dashboard cùng lúc"""
        overview = self.get_overview_stats()
        monthly_revenue = self.get_monthly_revenue()
        order_status = self.get_order_status_distribution()
        recent_activities = self.get_recent_activities(limit=5)
        top_selling = self.get_top_selling_items(limit=5)
        top_restaurants = self.get_top_revenue_restaurants(limit=5)

        return DashboardFullResponse(
            overview=overview,
            monthlyRevenue=monthly_revenue,
            orderStatus=order_status,
            recentActivities=recent_activities,
            topSelling=top_selling,
            topRestaurants=top_restaurants
        ).model_dump(by_alias=True)

    # ==================== LAYER 1: Shipper Dashboard - MongoDB Operations ====================

    def _aggregate_shipper_today_income(self, shipper_id: str, start_of_day: datetime) -> float:
        """Tính thu nhập hôm nay của shipper"""
        pipeline = [
            {
                "$match": {
                    "shipperId": ObjectId(shipper_id),
                    "status": OrderStatus.COMPLETED.value,
                    "updatedAt": {"$gte": start_of_day}
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$shipping_fee"}}}
        ]
        result = list(self.orders_collection.aggregate(pipeline))
        return float(result[0]["total"]) if result else 0.0

    def _calculate_shipper_working_hours(self, shipper_id: str, start_date: datetime, end_date: Optional[datetime] = None) -> float:
        """Tính tổng giờ làm việc dựa trên khoảng thời gian từ pickedAt đến updatedAt"""
        match_filter = {
            "shipperId": ObjectId(shipper_id),
            "status": OrderStatus.COMPLETED.value,
            "pickedAt": {"$ne": None, "$gte": start_date}
        }
        if end_date:
            match_filter["pickedAt"]["$lt"] = end_date
        
        # Lấy tất cả đơn hoàn thành có pickedAt
        orders = list(self.orders_collection.find(match_filter, {"pickedAt": 1, "updatedAt": 1}))
        
        total_hours = 0.0
        for order in orders:
            picked_at = order.get("pickedAt")
            updated_at = order.get("updatedAt")
            
            if picked_at and updated_at and isinstance(picked_at, datetime) and isinstance(updated_at, datetime):
                # Tính khoảng cách thời gian (giờ)
                time_diff = (updated_at - picked_at).total_seconds() / 3600
                total_hours += time_diff
        
        return round(total_hours, 1)

    def _count_shipper_today_completed_orders(self, shipper_id: str, start_of_day: datetime) -> int:
        """Đếm số đơn hoàn thành hôm nay của shipper"""
        return self.orders_collection.count_documents({
            "shipperId": ObjectId(shipper_id),
            "status": OrderStatus.COMPLETED.value,
            "updatedAt": {"$gte": start_of_day}
        })

    def _aggregate_shipper_month_stats(self, shipper_id: str, start_of_month: datetime, end_of_month: datetime) -> Dict:
        """Tính thống kê tháng của shipper"""
        pipeline = [
            {
                "$match": {
                    "shipperId": ObjectId(shipper_id),
                    "status": OrderStatus.COMPLETED.value,
                    "updatedAt": {"$gte": start_of_month, "$lt": end_of_month}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "orders": {"$sum": 1},
                    "revenue": {"$sum": "$shipping_fee"}
                }
            }
        ]
        result = list(self.orders_collection.aggregate(pipeline))
        if result:
            return {
                "orders": result[0]["orders"],
                "revenue": float(result[0]["revenue"])
            }
        return {"orders": 0, "revenue": 0.0}

    def _aggregate_shipper_total_income(self, shipper_id: str) -> float:
        """Tính tổng thu nhập tích lũy của shipper"""
        pipeline = [
            {
                "$match": {
                    "shipperId": ObjectId(shipper_id),
                    "status": OrderStatus.COMPLETED.value
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$shipping_fee"}}}
        ]
        result = list(self.orders_collection.aggregate(pipeline))
        return float(result[0]["total"]) if result else 0.0

    def _aggregate_shipper_monthly_revenue(self, shipper_id: str, year: int) -> Dict[int, Dict]:
        """Aggregate doanh thu shipper theo từng tháng trong năm"""
        pipeline = [
            {
                "$match": {
                    "shipperId": ObjectId(shipper_id),
                    "status": OrderStatus.COMPLETED.value,
                    "updatedAt": {
                        "$gte": datetime(year, 1, 1),
                        "$lt": datetime(year + 1, 1, 1)
                    }
                }
            },
            {
                "$group": {
                    "_id": {"$month": "$updatedAt"},
                    "orders": {"$sum": 1},
                    "revenue": {"$sum": "$shipping_fee"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        results = list(self.orders_collection.aggregate(pipeline))
        return {r["_id"]: {"orders": r["orders"], "revenue": float(r["revenue"])} for r in results}

    # ==================== LAYER 2: Shipper Dashboard - Business Logic ====================

    def get_shipper_overview_stats(self, shipper_id: str) -> Dict:
        """Lấy thống kê tổng quan hôm nay cho shipper"""
        now = datetime.now()
        start_of_day = datetime(now.year, now.month, now.day)

        # Sử dụng các hàm LAYER 1
        today_income = self._aggregate_shipper_today_income(shipper_id, start_of_day)
        today_completed = self._count_shipper_today_completed_orders(shipper_id, start_of_day)
        
        # Tính giờ hoạt động dựa trên thời gian thực tế (pickedAt -> updatedAt)
        today_hours = self._calculate_shipper_working_hours(shipper_id, start_of_day)

        return ShipperOverviewStats(
            todayIncome=today_income,
            todayCompletedOrders=today_completed,
            todayHours=today_hours
        ).model_dump(by_alias=True)

    def get_shipper_activity_history(self, shipper_id: str) -> Dict:
        """Lấy lịch sử hoạt động của shipper"""
        now = datetime.now()
        start_of_month = datetime(now.year, now.month, 1)
        if now.month == 12:
            end_of_month = datetime(now.year + 1, 1, 1)
        else:
            end_of_month = datetime(now.year, now.month + 1, 1)

        # Lấy dữ liệu từ LAYER 1
        month_stats = self._aggregate_shipper_month_stats(shipper_id, start_of_month, end_of_month)
        accumulated_income = self._aggregate_shipper_total_income(shipper_id)
        
        # Tính tổng giờ hoạt động trong tháng dựa trên thời gian thực tế
        total_hours = self._calculate_shipper_working_hours(shipper_id, start_of_month, end_of_month)

        return ShipperActivityHistory(
            monthOrders=month_stats["orders"],
            accumulatedIncome=accumulated_income,
            totalHours=total_hours
        ).model_dump(by_alias=True)

    def get_shipper_monthly_revenue(self, shipper_id: str, year: Optional[int] = None) -> List[Dict]:
        """Lấy doanh thu theo từng tháng trong năm cho shipper"""
        if year is None:
            year = datetime.now().year

        # Lấy dữ liệu từ LAYER 1
        revenue_map = self._aggregate_shipper_monthly_revenue(shipper_id, year)
        
        # Tạo data cho tất cả 12 tháng
        monthly_data = []
        for month in range(1, 13):
            data = revenue_map.get(month, {"orders": 0, "revenue": 0.0})
            monthly_data.append(
                ShipperMonthlyRevenueData(
                    month=f"T{month}",
                    orders=data["orders"],
                    revenue=data["revenue"]
                ).model_dump(by_alias=True)
            )

        return monthly_data

    def get_shipper_full_dashboard_data(self, shipper_id: str) -> Dict:
        """Lấy tất cả dữ liệu dashboard cho shipper cùng lúc"""
        overview = self.get_shipper_overview_stats(shipper_id)
        activity_history = self.get_shipper_activity_history(shipper_id)
        monthly_revenue = self.get_shipper_monthly_revenue(shipper_id)

        return ShipperDashboardFullResponse(
            overview=overview,
            activityHistory=activity_history,
            monthlyRevenue=monthly_revenue
        ).model_dump(by_alias=True)


# Singleton instance
dashboard_service = DashboardService()
