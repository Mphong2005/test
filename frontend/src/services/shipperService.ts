/**
 * Shipper Service
 * File này chứa các nghiệp vụ liên quan đến shipper
 * Bao gồm transform data từ backend sang frontend format
 */

import { OrderStatus, ShipperOrder, ShipperProfile } from '../types/shipper';
import { formatDateVN, formatDateISO } from '../utils/formatters';

/**
 * Map backend order status to frontend OrderStatus
 */
export function mapShipperOrderStatus(status: string): OrderStatus {
    const statusMap: Record<string, OrderStatus> = {
        'PENDING': OrderStatus.Pending,
        'SHIPPING': OrderStatus.Delivering,
        'COMPLETED': OrderStatus.Completed,
        'CANCELLED': OrderStatus.Cancelled,
    };
    return statusMap[status.toUpperCase()] || OrderStatus.Pending;
}

/**
 * Transform backend order data to ShipperOrder format
 */
export function transformToShipperOrder(order: any): ShipperOrder {
    const status = order.status || 'PENDING';

    // Helper function to format date to Vietnam time
    const formatToVietnamTime = (dateStr: string): string => {
        if (!dateStr) return '';
        try {
            const date = new Date(dateStr);
            if (isNaN(date.getTime())) return dateStr;

            const time = date.toLocaleTimeString('vi-VN', {
                timeZone: 'Asia/Ho_Chi_Minh',
                hour: '2-digit',
                minute: '2-digit',
                hour12: false
            });
            const dateFormatted = date.toLocaleDateString('vi-VN', {
                timeZone: 'Asia/Ho_Chi_Minh',
                day: '2-digit',
                month: '2-digit',
                year: 'numeric'
            });
            return `${time} - ${dateFormatted}`;
        } catch {
            return dateStr;
        }
    };

    // Thời gian đặt đơn (luôn là createdAt)
    const orderTimeStr = order.createdAt || order.created_at || '';
    const orderTime = formatToVietnamTime(orderTimeStr);

    // Thời gian hiển thị:
    // - Đơn COMPLETED: dùng updatedAt (thời gian giao xong)
    // - Đơn khác: dùng createdAt (thời gian đặt)
    let timeDisplay = '';
    const statusLower = status?.toLowerCase() || '';
    const isCompleted = statusLower === 'completed';

    if (isCompleted) {
        const deliveryTimeStr = order.updatedAt || order.updated_at || orderTimeStr;
        timeDisplay = formatToVietnamTime(deliveryTimeStr);
        console.log('[DEBUG] Completed order:', order._id, 'status:', status, 'createdAt:', orderTimeStr, 'updatedAt:', order.updatedAt, 'display:', timeDisplay);
    } else {
        timeDisplay = orderTime;
    }

    // Transform items
    const items = (order.items || []).map((item: any) => ({
        name: item.food_name || item.name || 'Unknown Item',
        quantity: item.quantity || 0,
        price: item.unit_price || item.price || 0,
    }));

    // Get customer info
    const customer = {
        name: order.userFullname || order.user_fullname || 'Khách hàng',
        phone: order.userPhone || order.user_phone || '090xxxxxxx',
        email: order.userEmail || order.user_email || '',
        avatar: order.userAvatar || order.user_avatar,
        rank: order.userRank || order.user_rank || 'Thành viên',
    };

    // Get food name from first item
    const firstFoodName = items.length > 0 ? items[0].name : '';

    return {
        id: order._id || order.order_id || order.id || '',
        storeName: order.restaurantName || order.restaurant_name || 'Nhà hàng',
        foodName: order.foodName || order.food_name || firstFoodName || '', // Tên món đầu tiên
        storeImage: order.imageUrl || order.image_url || 'https://images.unsplash.com/photo-1504674900247-0877df9cc836?w=400&h=300&fit=crop',
        storeAddress: order.restaurantAddress || order.restaurant_address || '',
        deliveryAddress: order.address || '',
        status: mapShipperOrderStatus(order.status || 'PENDING'),
        paymentMethod: (order.paymentMethod || order.payment_method || 'Cash') === 'COD' ? 'Cash' : 'Wallet',
        time: timeDisplay, // Thời gian giao (cho completed) hoặc thời gian đặt (cho pending/shipping)
        orderTime: orderTime, // Thời gian đặt đơn (luôn có)
        totalAmount: order.total_amount || order.totalAmount || 0,
        items,
        customer,
    };
}

/**
 * Transform an array of orders for shipper
 */
export function transformShipperOrders(orders: any[]): ShipperOrder[] {
    return orders.map(order => transformToShipperOrder(order));
}

/**
 * Transform backend user data to ShipperProfile format
 */
export function transformToShipperProfile(user: any): ShipperProfile {
    // Format created_at date for display (DD/MM/YYYY) using timezone-safe formatter
    const createdAt = user.created_at || user.createdAt;
    const joinDate = formatDateVN(createdAt);

    // Format birthday for date input (YYYY-MM-DD) using timezone-safe formatter
    const birthday = user.birthday || user.dob;
    const dob = formatDateISO(birthday);

    return {
        name: user.fullname || 'Shipper',
        email: user.email || '',
        avatar: user.avatar || 'https://images.unsplash.com/photo-1633332755192-727a05c4013d?w=400&q=60',
        rank: 'Tài xế 5 sao', // Default rank since backend doesn't have this field
        joinDate: joinDate !== 'Chưa cập nhật' ? joinDate : '',
        phone: user.phone_number || '',
        address: user.address || 'Hà Nội',
        dob: dob,
    };
}

/**
 * Map frontend profile update data to backend format
 */
export function mapProfileUpdateToBackend(profileData: {
    name?: string;
    email?: string;
    phone?: string;
    address?: string;
    dob?: string;
    gender?: string;
}): any {
    const updateData: any = {};
    if (profileData.name !== undefined) updateData.fullname = profileData.name;
    if (profileData.email !== undefined) updateData.email = profileData.email;
    if (profileData.phone !== undefined) updateData.phone_number = profileData.phone;
    if (profileData.address !== undefined) updateData.address = profileData.address;
    if (profileData.dob !== undefined && profileData.dob) {
        updateData.birthday = new Date(profileData.dob).toISOString();
    }
    if (profileData.gender !== undefined) {
        // Map Vietnamese gender to backend enum (Male/Female)
        if (profileData.gender === 'Nam') updateData.gender = 'Male';
        else if (profileData.gender === 'Nữ') updateData.gender = 'Female';
        else updateData.gender = profileData.gender;
    }
    return updateData;
}

/**
 * Calculate shipper statistics from orders data
 * Business logic for calculating today's income, completed count, and active hours
 */
export function calculateShipperStats(orders: any[]): { todayIncome: number; completedCount: number; activeHours: string } {
    const completedOrders = orders.filter((o: any) => (o.status || '').toLowerCase() === 'completed');

    // Calculate today's income and count
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const todayOrders = completedOrders.filter((o: any) => {
        const orderDate = new Date(o.createdAt || o.created_at);
        orderDate.setHours(0, 0, 0, 0);
        return orderDate.getTime() === today.getTime();
    });

    const todayIncome = todayOrders.reduce((sum: number, o: any) => sum + (o.total_amount || 0), 0);
    const completedCount = todayOrders.length;

    // Calculate active hours (simplified)
    const activeHours = '5h 34p';

    return {
        todayIncome,
        completedCount,
        activeHours,
    };
}
