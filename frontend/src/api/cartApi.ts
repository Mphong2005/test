/**
 * Cart API
 * Kết nối với Backend API Cart
 */

import { getBackendBaseUrl, apiClient } from './axiosClient';

const CART_BASE_URL = `${getBackendBaseUrl()}/api/cart`;

/**
 * Get cart (User only)
 */
export const getCartApi = async () => {
  try {
    const response = await apiClient.get(`${CART_BASE_URL}`);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    return null;
  } catch (error: any) {
    console.error('Error fetching cart:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải giỏ hàng');
  }
};

/**
 * Add item to cart
 */
export const addToCartApi = async (restaurantId: string, foodName: string, quantity: number = 1) => {
  try {
    const response = await apiClient.post(`${CART_BASE_URL}/items`, {
      restaurantId,
      foodName,
      quantity,
    });
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error('Failed to add to cart');
  } catch (error: any) {
    console.error('Error adding to cart:', error);
    throw new Error(error.response?.data?.message || 'Không thể thêm vào giỏ hàng');
  }
};

/**
 * Update cart item quantity
 */
export const updateCartItemApi = async (foodName: string, quantity: number) => {
  try {
    const response = await apiClient.put(`${CART_BASE_URL}/items/${encodeURIComponent(foodName)}`, {
      quantity,
    });
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error('Failed to update cart item');
  } catch (error: any) {
    console.error('Error updating cart item:', error);
    throw new Error(error.response?.data?.message || 'Không thể cập nhật giỏ hàng');
  }
};

/**
 * Remove item from cart
 */
export const removeFromCartApi = async (foodName: string) => {
  try {
    const response = await apiClient.delete(`${CART_BASE_URL}/items/${encodeURIComponent(foodName)}`);
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error('Failed to remove from cart');
  } catch (error: any) {
    console.error('Error removing from cart:', error);
    throw new Error(error.response?.data?.message || 'Không thể xóa món khỏi giỏ hàng');
  }
};

/**
 * Clear cart
 */
export const clearCartApi = async () => {
  try {
    const response = await apiClient.delete(`${CART_BASE_URL}`);
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error('Failed to clear cart');
  } catch (error: any) {
    console.error('Error clearing cart:', error);
    throw new Error(error.response?.data?.message || 'Không thể xóa giỏ hàng');
  }
};

