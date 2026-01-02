/**
 * Order Service
 * File này chứa các nghiệp vụ liên quan đến đơn hàng
 * Bao gồm transform data từ backend sang frontend format
 */

import { Order } from '../types/common';

/**
 * Default placeholder image for orders without images
 */
const DEFAULT_ORDER_IMAGE = 'https://images.unsplash.com/photo-1504674900247-0877df9cc836?w=400&h=300&fit=crop';

/**
 * Map backend order status to frontend format
 */
export function mapOrderStatus(status: string): 'PENDING' | 'DELIVERING' | 'COMPLETED' | 'CANCELLED' {
    const statusMap: Record<string, 'PENDING' | 'DELIVERING' | 'COMPLETED' | 'CANCELLED'> = {
        'PENDING': 'PENDING',
        'SHIPPING': 'DELIVERING',
        'COMPLETED': 'COMPLETED',
        'CANCELLED': 'CANCELLED',
    };
    return statusMap[status.toUpperCase()] || 'PENDING';
}

/**
 * Transform backend order data to frontend format
 * Backend returns: _id, restaurantName, restaurantId, total_amount, status, createdAt, items, etc.
 */
export function transformOrderData(order: any): Order {
    // Build description from items
    const items = order.items || [];
    const description = items.length > 0
        ? items.map((item: any) => `${item.food_name} x${item.quantity}`).join(', ')
        : 'Không có thông tin món';

    // Parse date - Backend lưu UTC, frontend convert sang Vietnam time (UTC+7)
    // Dùng JavaScript Date để parse và convert timezone tự động
    let orderTime = '';
    const dateStr = order.createdAt || order.created_at;
    if (dateStr) {
        try {
            // Parse date string - JavaScript sẽ tự động hiểu timezone từ ISO/RFC format
            const date = new Date(dateStr);
            if (!isNaN(date.getTime())) {
                // Format sang Vietnam timezone
                const vietnamTime = date.toLocaleString('vi-VN', {
                    timeZone: 'Asia/Ho_Chi_Minh',
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit',
                    day: '2-digit',
                    month: '2-digit',
                    year: 'numeric',
                    hour12: false
                });
                orderTime = `Đặt lúc ${vietnamTime}`;
            } else {
                orderTime = `Đặt lúc ${dateStr}`;
            }
        } catch {
            orderTime = `Đặt lúc ${dateStr}`;
        }
    }

    // Build foodId for product page navigation: {restaurantId}-{foodName}
    const restaurantId = order.restaurantId || order.restaurant_id || '';
    const firstFoodName = items[0]?.food_name || '';
    const foodId = restaurantId && firstFoodName ? `${restaurantId}-${firstFoodName}` : '';

    // Determine review status
    const mappedStatus = mapOrderStatus(order.status || 'Pending');
    // needsReview: đơn COMPLETED cần người dùng đánh giá
    // isReviewed: đơn đã được đánh giá xong (hiển thị trong lịch sử "Tất cả")
    const isReviewed = order.isReviewed || false;
    const needsReview = order.needsReview || false;

    return {
        id: order._id || order.order_id || order.id || '',
        foodId,
        restaurantName: order.restaurantName || order.restaurant_name || 'Nhà hàng',
        foodName: firstFoodName || '', // Tên món đầu tiên để hiển thị
        orderTime,
        description,
        totalAmount: order.total_amount ?? order.totalAmount ?? 0,
        status: mappedStatus,
        imageUrl: order.imageUrl || order.image_url || DEFAULT_ORDER_IMAGE,
        isReviewed,
        needsReview,
        customer: order.userFullname || order.user_fullname || order.customer || 'Khách lẻ',
        email: order.userEmail || order.user_email || order.email,
        phone: order.userPhone || order.user_phone || order.phone || order.phone_number,
        address: order.address || order.delivery_address || order.shipping_address,
        items: items.map((item: any) => ({
            food_name: item.food_name || item.foodName || '',
            quantity: item.quantity || 1,
            unit_price: item.unit_price || item.unitPrice || 0,
        })),
        shipper: order.shipper ? {
            shipperId: order.shipper.shipperId || order.shipper.shipper_id || '',
            fullname: order.shipper.fullname || '',
            phone_number: order.shipper.phone_number || order.shipper.phone || '',
            email: order.shipper.email || '',
        } : undefined,
    };
}

/**
 * Transform an array of orders
 */
export function transformOrdersData(orders: any[]): Order[] {
    return orders.map(order => transformOrderData(order));
}
