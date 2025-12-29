from flask import jsonify, request
from services.dashboard_service import dashboard_service


class DashboardController:
    """Controller xử lý các request liên quan đến dashboard"""

    def __init__(self):
        self.service = dashboard_service

    def get_overview(self):
        """
        GET /api/dashboard/overview
        Lấy thống kê tổng quan
        """
        try:
            data = self.service.get_overview_stats()
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy thống kê tổng quan: {str(e)}'
            }), 500

    def get_monthly_revenue(self):
        """
        GET /api/dashboard/monthly-revenue?year=2025
        Lấy doanh thu theo tháng
        """
        try:
            year = request.args.get('year', type=int)
            data = self.service.get_monthly_revenue(year)
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy doanh thu theo tháng: {str(e)}'
            }), 500

    def get_order_status_distribution(self):
        """
        GET /api/dashboard/order-status
        Lấy phân bố trạng thái đơn hàng
        """
        try:
            data = self.service.get_order_status_distribution()
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy phân bố trạng thái: {str(e)}'
            }), 500

    def get_recent_activities(self):
        """
        GET /api/dashboard/recent-activities?limit=10
        Lấy hoạt động gần đây
        """
        try:
            limit = request.args.get('limit', default=10, type=int)
            data = self.service.get_recent_activities(limit)
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy hoạt động gần đây: {str(e)}'
            }), 500

    def get_top_selling(self):
        """
        GET /api/dashboard/top-selling?limit=10
        Lấy top món bán chạy
        """
        try:
            limit = request.args.get('limit', default=10, type=int)
            data = self.service.get_top_selling_items(limit)
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy top món bán chạy: {str(e)}'
            }), 500

    def get_top_restaurants(self):
        """
        GET /api/dashboard/top-restaurants?limit=10
        Lấy top nhà hàng theo doanh thu
        """
        try:
            limit = request.args.get('limit', default=10, type=int)
            data = self.service.get_top_revenue_restaurants(limit)
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy top nhà hàng: {str(e)}'
            }), 500

    def get_full_dashboard(self):
        """
        GET /api/dashboard/full
        Lấy tất cả dữ liệu dashboard cùng lúc (tối ưu cho 1 request)
        """
        try:
            data = self.service.get_full_dashboard_data()
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy dữ liệu dashboard: {str(e)}'
            }), 500

    # ==================== Shipper Dashboard Methods ====================

    def get_shipper_overview(self):
        """
        GET /api/dashboard/shipper/overview
        Lấy thống kê tổng quan hôm nay cho shipper
        """
        try:
            shipper_id = request.user_id
            data = self.service.get_shipper_overview_stats(shipper_id)
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy thống kê shipper: {str(e)}'
            }), 500

    def get_shipper_activity(self):
        """
        GET /api/dashboard/shipper/activity
        Lấy lịch sử hoạt động của shipper
        """
        try:
            shipper_id = request.user_id
            data = self.service.get_shipper_activity_history(shipper_id)
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy lịch sử hoạt động: {str(e)}'
            }), 500

    def get_shipper_monthly_revenue(self):
        """
        GET /api/dashboard/shipper/monthly-revenue?year=2025
        Lấy doanh thu theo tháng của shipper
        """
        try:
            shipper_id = request.user_id
            year = request.args.get('year', type=int)
            data = self.service.get_shipper_monthly_revenue(shipper_id, year)
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy doanh thu theo tháng: {str(e)}'
            }), 500

    def get_shipper_full_dashboard(self):
        """
        GET /api/dashboard/shipper/full
        Lấy tất cả dữ liệu dashboard cho shipper cùng lúc
        """
        try:
            shipper_id = request.user_id
            data = self.service.get_shipper_full_dashboard_data(shipper_id)
            return jsonify({
                'success': True,
                'data': data
            }), 200
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Lỗi khi lấy dashboard shipper: {str(e)}'
            }), 500


# Singleton instance
dashboard_controller = DashboardController()
