/**
 * Shipper API
 * Kết nối UI Shipper với Backend API
 * Chỉ chứa các HTTP calls, logic transform data nằm ở shipperService
 */

import { apiClient, getBackendBaseUrl } from './axiosClient';
import { OrderStatus, ShipperOrder, ShipperProfile } from '../types/shipper';
import {
  transformToShipperOrder,
  transformShipperOrders,
  transformToShipperProfile,
  mapProfileUpdateToBackend,
  calculateShipperStats
} from '../services/shipperService';

const ORDER_BASE_URL = `${getBackendBaseUrl()}/api/orders`;

/**
 * Get pending orders (orders with status PENDING - "Đang chuẩn bị")
 * These are orders waiting for shipper to accept
 */
export const getShipperOrdersApi = async (status?: OrderStatus | 'HISTORY'): Promise<ShipperOrder[]> => {
  try {
    let response;

    if (status === 'HISTORY') {
      // Get all shipper's orders and filter to only Completed and Cancelled on frontend
      const allOrdersResponse = await apiClient.get(`${ORDER_BASE_URL}/shipper/my_deliveries`);
      if (allOrdersResponse.data.success && allOrdersResponse.data.data) {
        const filteredOrders = allOrdersResponse.data.data.filter((order: any) => {
          const orderStatus = (order.status || '').toLowerCase();
          return orderStatus === 'completed' || orderStatus === 'cancelled';
        });
        return transformShipperOrders(filteredOrders);
      }
      return [];
    } else if (status === OrderStatus.Pending) {
      response = await apiClient.get(`${ORDER_BASE_URL}/shipper/pending`);
    } else if (status === OrderStatus.Delivering) {
      response = await apiClient.get(`${ORDER_BASE_URL}/shipper/filter`, {
        params: { status: 'Shipping' },
      });
    } else if (status === OrderStatus.Completed) {
      response = await apiClient.get(`${ORDER_BASE_URL}/shipper/filter`, {
        params: { status: 'Completed' },
      });
    } else {
      response = await apiClient.get(`${ORDER_BASE_URL}/shipper/my_deliveries`);
    }

    if (response && response.data.success && response.data.data) {
      return transformShipperOrders(response.data.data);
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching shipper orders:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải danh sách đơn hàng');
  }
};

/**
 * Shipper accepts an order (PENDING → SHIPPING)
 */
export const acceptOrderApi = async (orderId: string): Promise<boolean> => {
  try {
    const response = await apiClient.put(`${ORDER_BASE_URL}/${orderId}/accept`);
    if (response.data.success) {
      return true;
    }
    return false;
  } catch (error: any) {
    console.error('Error accepting order:', error);
    throw new Error(error.response?.data?.message || 'Không thể nhận đơn hàng');
  }
};

/**
 * Shipper completes an order (SHIPPING → COMPLETED)
 */
export const completeOrderApi = async (orderId: string): Promise<boolean> => {
  try {
    const response = await apiClient.put(`${ORDER_BASE_URL}/${orderId}/completed`);
    if (response.data.success) {
      return true;
    }
    return false;
  } catch (error: any) {
    console.error('Error completing order:', error);
    throw new Error(error.response?.data?.message || 'Không thể hoàn thành đơn hàng');
  }
};

/**
 * Shipper declines a pending order (chưa nhận)
 */
export const declineOrderApi = async (orderId: string, reason?: string): Promise<boolean> => {
  try {
    const response = await apiClient.put(`${ORDER_BASE_URL}/${orderId}/decline`, {
      reason: reason || 'Shipper từ chối đơn hàng',
    });
    if (response.data.success) {
      return true;
    }
    return false;
  } catch (error: any) {
    console.error('Error declining order:', error);
    throw new Error(error.response?.data?.message || 'Không thể từ chối đơn hàng');
  }
};

/**
 * Shipper rejects an order that was already accepted (SHIPPING → PENDING)
 */
export const rejectOrderApi = async (orderId: string, reason?: string): Promise<boolean> => {
  try {
    const response = await apiClient.put(`${ORDER_BASE_URL}/${orderId}/reject`, {
      reason: reason || 'Shipper từ chối đơn hàng',
    });
    if (response.data.success) {
      return true;
    }
    return false;
  } catch (error: any) {
    console.error('Error rejecting order:', error);
    throw new Error(error.response?.data?.message || 'Không thể từ chối đơn hàng');
  }
};

/**
 * Get shipper statistics
 */
export const getShipperStatsApi = async (): Promise<{ todayIncome: number; completedCount: number; activeHours: string }> => {
  try {
    const response = await apiClient.get(`${ORDER_BASE_URL}/shipper/my_deliveries`);

    if (response.data.success && response.data.data) {
      return calculateShipperStats(response.data.data);
    }

    return { todayIncome: 0, completedCount: 0, activeHours: '5h 34p' };
  } catch (error: any) {
    console.error('Error fetching shipper stats:', error);
    return { todayIncome: 0, completedCount: 0, activeHours: '5h 34p' };
  }
};

/**
 * Get shipper profile
 */
export const getShipperProfileApi = async (): Promise<ShipperProfile> => {
  try {
    const response = await apiClient.get(`${getBackendBaseUrl()}/api/users/profile_me`);

    if (response.data.success && response.data.data) {
      return transformToShipperProfile(response.data.data);
    }

    throw new Error('Không thể lấy thông tin shipper');
  } catch (error: any) {
    console.error('Error fetching shipper profile:', error);
    throw new Error(error.response?.data?.message || 'Không thể lấy thông tin shipper');
  }
};

/**
 * Update shipper profile
 */
export const updateShipperProfileApi = async (profileData: {
  name?: string;
  email?: string;
  phone?: string;
  address?: string;
  dob?: string;
  gender?: string;
}): Promise<ShipperProfile> => {
  try {
    const updateData = mapProfileUpdateToBackend(profileData);

    const response = await apiClient.put(`${getBackendBaseUrl()}/api/users/update_profile`, updateData);
    if (response.data.success && response.data.data) {
      return transformToShipperProfile(response.data.data);
    }
    throw new Error('Failed to update shipper profile');
  } catch (error: any) {
    console.error('Error updating shipper profile:', error);
    throw new Error(error.response?.data?.message || 'Không thể cập nhật thông tin shipper');
  }
};

