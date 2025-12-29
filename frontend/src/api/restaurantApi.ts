/**
 * Restaurant API
 * Kết nối với Backend API Restaurant
 * Chỉ chứa các HTTP calls, logic transform data nằm ở restaurantService
 */

import { getBackendBaseUrl, apiClient } from './axiosClient';
import {
  transformRestaurantListItem,
  transformRestaurantsList,
  transformRestaurantDetail,
  transformMenuToFoodList
} from '../services/restaurantService';

const RESTAURANT_BASE_URL = `${getBackendBaseUrl()}/api/restaurants`;

/**
 * Get all restaurants (Admin only - includes inactive)
 */
export const getRestaurantsApi = async () => {
  try {
    const response = await apiClient.get(`${RESTAURANT_BASE_URL}/admin/all`);
    if (response.data.success && response.data.data) {
      return transformRestaurantsList(response.data.data);
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching restaurants:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải danh sách nhà hàng');
  }
};

/**
 * Get restaurant by ID (Public - for users, only active restaurants)
 * Uses fetch to avoid auth requirements
 * For admin menu view, use admin endpoint to get full menu
 */
export const getRestaurantByIdApi = async (id: string, useAdminEndpoint: boolean = false) => {
  try {
    const backendUrl = getBackendBaseUrl();
    let response;
    let data;

    if (useAdminEndpoint) {
      // Use admin endpoint for admin menu view
      try {
        response = await apiClient.get(`${RESTAURANT_BASE_URL}/admin/${id}`);
        data = response.data;
      } catch (adminError: any) {
        console.error('Admin endpoint failed:', adminError);
        throw adminError;
      }
    } else {
      // Try public endpoint first (for users)
      try {
        const fetchResponse = await fetch(`${backendUrl}/api/restaurants/${id}`, {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
        });
        if (!fetchResponse.ok) {
          throw new Error(`HTTP error! status: ${fetchResponse.status}`);
        }
        data = await fetchResponse.json();
      } catch (publicError: any) {
        // If public endpoint fails, try admin endpoint with auth
        try {
          response = await apiClient.get(`${RESTAURANT_BASE_URL}/admin/${id}`);
          data = response.data;
        } catch (adminError) {
          throw publicError;
        }
      }
    }

    if (data && data.success && data.data) {
      const restaurant = data.data;
      console.log('Restaurant detail response:', restaurant);
      console.log('Restaurant menu:', restaurant.menu);
      console.log('Menu type:', typeof restaurant.menu, Array.isArray(restaurant.menu));
      if (restaurant.menu && Array.isArray(restaurant.menu)) {
        console.log('Menu length:', restaurant.menu.length);
        restaurant.menu.forEach((cat: any, idx: number) => {
          console.log(`Category ${idx}:`, cat.category, 'items:', cat.items?.length || 0);
        });
      }

      const transformed = transformRestaurantDetail(restaurant);
      console.log('Transformed restaurant:', {
        id: transformed.id,
        name: transformed.name,
        address: transformed.address,
        menuLength: transformed.menu.length,
      });

      return transformed;
    }
    throw new Error('Restaurant not found');
  } catch (error: any) {
    console.error('Error fetching restaurant:', error);
    throw new Error(error.response?.data?.message || error.message || 'Không thể tải thông tin nhà hàng');
  }
};

/**
 * Create restaurant (Admin only)
 */
export const createRestaurantApi = async (restaurantData: any) => {
  try {
    const response = await apiClient.post(`${RESTAURANT_BASE_URL}`, restaurantData);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error('Failed to create restaurant');
  } catch (error: any) {
    console.error('Error creating restaurant:', error);
    throw new Error(error.response?.data?.message || 'Không thể tạo nhà hàng');
  }
};

/**
 * Update restaurant (Admin only)
 */
export const updateRestaurantApi = async (id: string, restaurantData: any) => {
  try {
    const response = await apiClient.put(`${RESTAURANT_BASE_URL}/${id}`, restaurantData);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error('Failed to update restaurant');
  } catch (error: any) {
    console.error('Error updating restaurant:', error);
    throw new Error(error.response?.data?.message || 'Không thể cập nhật nhà hàng');
  }
};

/**
 * Delete restaurant (Admin only)
 */
export const deleteRestaurantApi = async (id: string) => {
  try {
    const response = await apiClient.delete(`${RESTAURANT_BASE_URL}/${id}`);
    if (response.data.success) {
      return;
    }
    throw new Error('Failed to delete restaurant');
  } catch (error: any) {
    console.error('Error deleting restaurant:', error);
    throw new Error(error.response?.data?.message || 'Không thể xóa nhà hàng');
  }
};

/**
 * Toggle restaurant status (Admin only)
 */
export const updateRestaurantStatusApi = async (id: string, status: string) => {
  try {
    const isActive = status === 'Active';
    const response = await apiClient.put(`${RESTAURANT_BASE_URL}/${id}/toggle-status`, {
      status: isActive,
    });
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error('Failed to update restaurant status');
  } catch (error: any) {
    console.error('Error updating restaurant status:', error);
    throw new Error(error.response?.data?.message || 'Không thể cập nhật trạng thái nhà hàng');
  }
};

/**
 * Get restaurant menu (Admin)
 * Transforms menu from backend format (array of categories with items) to flat array of food items
 */
export const getRestaurantMenuApi = async (restaurantId: string): Promise<any[]> => {
  try {
    // Use admin endpoint to get full menu (including inactive restaurants)
    const restaurant = await getRestaurantByIdApi(restaurantId, true);
    const menu = restaurant.menu || [];

    console.log('getRestaurantMenuApi - Menu received:', menu);
    console.log('getRestaurantMenuApi - Menu is array:', Array.isArray(menu));
    console.log('getRestaurantMenuApi - Menu length:', menu.length);

    const foods = transformMenuToFoodList(menu);
    console.log('getRestaurantMenuApi - Total foods:', foods.length);

    return foods;
  } catch (error: any) {
    console.error('Error fetching restaurant menu:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải menu nhà hàng');
  }
};

/**
 * Search restaurants (Admin - includes all restaurants)
 */
export const searchRestaurantsApi = async (query: string) => {
  try {
    const response = await apiClient.get(`${RESTAURANT_BASE_URL}/search_admin`, {
      params: { q: query },
    });
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    return [];
  } catch (error: any) {
    console.error('Error searching restaurants:', error);
    throw new Error(error.response?.data?.message || 'Không thể tìm kiếm nhà hàng');
  }
};

