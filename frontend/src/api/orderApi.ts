/**
 * Order API
 * Kết nối với Backend API Order
 * Chỉ chứa các HTTP calls, logic transform data nằm ở orderService
 */

import { getBackendBaseUrl, apiClient } from './axiosClient';
import { Order } from '../types/common';
import { transformOrderData, transformOrdersData } from '../services/orderService';

const ORDER_BASE_URL = `${getBackendBaseUrl()}/api/orders`;

/**
 * Get all orders (Admin only)
 */
export async function getAllOrdersApi(): Promise<Order[]> {
  try {
    const response = await apiClient.get(`${ORDER_BASE_URL}/all`);
    if (response.data.success && response.data.data) {
      return transformOrdersData(response.data.data);
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching orders:', error);
    const errorMessage = error.message || error.response?.data?.message || 'Không thể tải danh sách đơn hàng';
    throw new Error(errorMessage);
  }
}

/**
 * Get current user's orders (User only)
 */
export async function getMyOrdersApi(): Promise<Order[]> {
  try {
    const response = await apiClient.get(`${ORDER_BASE_URL}/my_orders`);
    if (response.data.success && response.data.data) {
      return transformOrdersData(response.data.data);
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching my orders:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải danh sách đơn hàng');
  }
}

/**
 * Get orders filtered by status (Admin only)
 */
export const getOrdersByStatusApi = async (status: string): Promise<Order[]> => {
  try {
    const response = await apiClient.get(`${ORDER_BASE_URL}/all/filter`, {
      params: { status },
    });
    if (response.data.success && response.data.data) {
      return transformOrdersData(response.data.data);
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching orders by status:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải đơn hàng');
  }
};

/**
 * Cancel order (User - PENDING only)
 */
export const userCancelOrderApi = async (orderId: string, reason?: string) => {
  try {
    const payload: any = {};
    if (reason) {
      payload.reason = reason;
    }
    const response = await apiClient.put(`${ORDER_BASE_URL}/${orderId}/cancel`, payload);
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error('Failed to cancel order');
  } catch (error: any) {
    console.error('Error canceling order:', error);
    throw new Error(error.response?.data?.message || 'Không thể hủy đơn hàng');
  }
};

/**
 * Cancel order (Admin only - can cancel any status)
 */
export const cancelOrderApi = async (orderId: string, reason?: string) => {
  try {
    const response = await apiClient.put(`${ORDER_BASE_URL}/${orderId}/admin_cancel`, {
      reason,
    });
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error('Failed to cancel order');
  } catch (error: any) {
    console.error('Error canceling order:', error);
    throw new Error(error.response?.data?.message || 'Không thể hủy đơn hàng');
  }
};

/**
 * Delete order (Note: Backend may not have this endpoint, keeping for compatibility)
 */
export const deleteOrderApi = async (id: string) => {
  console.warn('Delete order endpoint not available in backend');
  throw new Error('Không thể xóa đơn hàng. Vui lòng sử dụng chức năng hủy đơn.');
};

/**
 * Get orders by restaurant (Admin only)
 */
export const getRestaurantOrdersApi = async (restaurantId: string): Promise<Order[]> => {
  try {
    const response = await apiClient.get(`${ORDER_BASE_URL}/restaurant/${restaurantId}`);
    if (response.data.success && response.data.data) {
      return transformOrdersData(response.data.data);
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching restaurant orders:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải đơn hàng của nhà hàng');
  }
};

/**
 * Create order (User only - kept for compatibility)
 */
export const createOrderApi = async (orderData: any) => {
  try {
    const response = await apiClient.post(`${ORDER_BASE_URL}`, orderData);
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error('Failed to create order');
  } catch (error: any) {
    console.error('Error creating order:', error);
    const errorMessage = error.message || error.response?.data?.message || 'Không thể tạo đơn hàng';
    throw new Error(errorMessage);
  }
};

