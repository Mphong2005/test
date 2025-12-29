/**
 * Dashboard Service
 * File này chứa các nghiệp vụ liên quan đến dashboard
 * Bao gồm transform data từ backend sang frontend format
 */

/**
 * Transform dashboard data from backend to frontend format
 */
export function transformDashboardData(dashboardData: any, revenueData?: any[]) {
    const overview = dashboardData.overview || {};
    const orderStatus = dashboardData.orderStatus || dashboardData.order_status || {};
    const recentActivities = dashboardData.recentActivities || dashboardData.recent_activities || [];
    const topSelling = dashboardData.topSelling || dashboardData.top_selling || [];
    const topRestaurants = dashboardData.topRestaurants || dashboardData.top_restaurants || [];

    // Use provided revenueData or fallback to dashboard data
    const revenue = revenueData || dashboardData.monthlyRevenue?.data || dashboardData.monthly_revenue?.data || [];

    return {
        summary: transformOverview(overview),
        revenue: transformRevenueData(revenue),
        status: transformOrderStatus(orderStatus),
        activities: transformRecentActivities(recentActivities),
        topItems: transformTopSelling(topSelling),
        topRestaurants: transformTopRestaurants(topRestaurants),
    };
}

/**
 * Transform overview data
 */
export function transformOverview(overview: any) {
    return {
        totalRevenue: overview.totalRevenueMonth || overview.total_revenue_month || 0,
        todayRevenue: overview.revenueToday || overview.revenue_today || 0,
        totalUsers: overview.activeUsers || overview.active_users || 0,
        totalRestaurants: overview.totalRestaurants || overview.total_restaurants || 0,
    };
}

/**
 * Transform revenue data
 */
export function transformRevenueData(revenueData: any[]) {
    return revenueData.map((item: any) => ({
        name: item.month || item.name,
        value: item.revenue || item.value || 0,
    }));
}

/**
 * Transform order status distribution
 */
export function transformOrderStatus(orderStatus: any) {
    return [
        { name: 'Hoàn thành', value: orderStatus.completed || 0, color: '#10B981' },
        { name: 'Đang chờ', value: orderStatus.pending || 0, color: '#F59E0B' },
        { name: 'Đang giao', value: orderStatus.shipping || 0, color: '#3B82F6' },
        { name: 'Đã hủy', value: orderStatus.cancelled || 0, color: '#EF4444' },
    ];
}

/**
 * Transform recent activities
 */
export function transformRecentActivities(activities: any[]) {
    return activities.map((act: any) => ({
        id: act.orderId || act.order_id || '',
        user: act.userName || act.user_name || 'Unknown',
        action: act.type || 'Unknown',
        target: act.orderId || act.order_id || '',
        time: act.timestamp ? new Date(act.timestamp).toLocaleString('vi-VN') : '',
        type: act.type?.toLowerCase() || 'order',
    }));
}

/**
 * Transform top selling items
 */
export function transformTopSelling(topSelling: any[]) {
    return topSelling.map((item: any, index: number) => {
        const foodName = item.foodName || item.food_name || '';
        const restaurantId = item.restaurantId || item.restaurant_id || '';
        // Tạo food ID từ restaurantId và foodName (format: restaurantId-foodName)
        const foodId = restaurantId && foodName
            ? `${restaurantId}-${foodName}`.replace(/#RES-/g, '').replace(/\s+/g, '-').toLowerCase()
            : `item-${index}`;

        return {
            id: foodId,
            foodId: foodId,
            name: foodName,
            restaurantName: item.restaurantName || item.restaurant_name || '',
            restaurantId: restaurantId,
            sales: item.totalSold || item.total_sold || 0,
            image: item.image || null, // Backend không trả về image, sẽ cần lấy từ restaurant menu
        };
    });
}

/**
 * Transform top restaurants
 */
export function transformTopRestaurants(topRestaurants: any[]) {
    const colors = [
        'bg-red-50 text-red-600',
        'bg-blue-50 text-blue-600',
        'bg-green-50 text-green-600',
        'bg-orange-50 text-orange-600',
        'bg-purple-50 text-purple-600',
    ];

    return topRestaurants.map((res: any, index: number) => ({
        id: res.restaurantId || res.restaurant_id || `res-${index}`,
        name: res.restaurantName || res.restaurant_name || '',
        revenue: res.totalRevenue || res.total_revenue || 0,
        logoInitial: (res.restaurantName || res.restaurant_name || 'R').charAt(0).toUpperCase(),
        color: colors[index % colors.length],
    }));
}
