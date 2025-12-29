/**
 * User API
 * File này chứa các API endpoints liên quan đến quản lý người dùng
 */

import { getBackendBaseUrl, getUsersApiBaseUrl, requestJson, getAuthHeaders, apiClient } from './axiosClient';
import { BackendUser as AdminUser, CreateUserRequest, UpdateUserRequest } from '../types/user';
import { getToken } from '../utils/storage';

// Re-export for backward compatibility
export type { BackendUser as AdminUser, CreateUserRequest, UpdateUserRequest } from '../types/user';

/**
 * Get all users (Admin only)
 * @returns Promise<AdminUser[]>
 */
export const getAllUsersApi = async (): Promise<AdminUser[]> => {
  const token = getToken();
  if (!token) {
    throw new Error('Chưa đăng nhập. Vui lòng đăng nhập lại.');
  }

  try {
    console.log('Fetching users from:', `${getUsersApiBaseUrl()}/all`);
    console.log('Token:', token.substring(0, 20) + '...');

    const response = await fetch(`${getUsersApiBaseUrl()}/all`, {
      method: 'GET',
      headers: getAuthHeaders(),
    });

    console.log('Response status:', response.status);

    const data = await response.json().catch((err) => {
      console.error('Error parsing JSON:', err);
      throw new Error('Không thể parse response từ server');
    });

    console.log('Response data:', data);

    // Check if response is an error
    if (!response.ok) {
      const errorMessage = data.message || `Lỗi ${response.status}: ${response.statusText}`;
      throw new Error(errorMessage);
    }

    // Handle different response formats
    if (Array.isArray(data)) {
      return data;
    }
    if (data.data && Array.isArray(data.data)) {
      return data.data;
    }
    if (data.success && Array.isArray(data.data)) {
      return data.data;
    }

    console.error('Unexpected response format:', data);
    throw new Error('Format dữ liệu không đúng từ server');
  } catch (error) {
    console.error('Error fetching users:', error);
    if (error instanceof Error) {
      throw error;
    }
    throw new Error('Không thể tải danh sách người dùng');
  }
};

/**
 * Delete user by ID (Admin only)
 * @param userId - User ID to delete
 */
export const deleteUserApi = async (userId: string): Promise<void> => {
  const token = getToken();
  if (!token) {
    throw new Error('Chưa đăng nhập');
  }

  await requestJson(
    `${getUsersApiBaseUrl()}/${userId}`,
    {
      method: 'DELETE',
      headers: getAuthHeaders(),
    }
  );
};

/**
 * Update user role (Admin only)
 * @param userId - User ID
 * @param role - New role
 * @returns Updated user
 */
export const updateUserRoleApi = async (userId: string, role: 'admin' | 'user' | 'shipper'): Promise<AdminUser> => {
  const token = getToken();
  if (!token) {
    throw new Error('Chưa đăng nhập');
  }

  const data = await requestJson<{ data: AdminUser }>(
    `${getUsersApiBaseUrl()}/role/${userId}`,
    {
      method: 'PUT',
      headers: getAuthHeaders(),
      body: JSON.stringify({ role }),
    }
  );

  return data.data;
};

/**
 * Create new user (Uses register endpoint)
 * @param userData - User data to create
 * @returns Created user
 */
export const createUserApi = async (userData: CreateUserRequest): Promise<AdminUser> => {
  // Use register endpoint for creating users (doesn't require auth)
  // Note: Backend register always creates user with role 'user', so we'll need to update role after
  const data = await requestJson<{ data: { user: AdminUser; token: string } }>(
    `${getBackendBaseUrl()}/api/auth/register`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        fullname: userData.fullname,
        email: userData.email,
        password: userData.password,
        phone_number: userData.phone_number,
        birthday: userData.birthday ? new Date(userData.birthday).toISOString() : undefined,
        gender: userData.gender,
      }),
    }
  );

  const createdUser = data.data.user;

  // If role is specified and different from default 'user', update it
  if (userData.role && userData.role !== 'user') {
    const token = getToken();
    if (token) {
      try {
        const updatedUser = await updateUserRoleApi(createdUser.user_id, userData.role);
        return updatedUser;
      } catch (err) {
        console.error('Error updating user role:', err);
        // Return the created user even if role update fails
      }
    }
  }

  return createdUser;
};

/**
 * Get current user profile
 * @returns User profile
 */
export const getUserProfileApi = async (): Promise<AdminUser> => {
  try {
    const response = await apiClient.get(`${getUsersApiBaseUrl()}/profile_me`);
    if (response.data.success && response.data.data) {
      // Return raw data - transformation is handled by userService
      return response.data.data;
    }
    throw new Error('Failed to get user profile');
  } catch (error: any) {
    console.error('Error getting user profile:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải thông tin người dùng');
  }
};

/**
 * Update user profile (User updating own profile)
 * @param userData - User data to update
 * @returns Updated user
 */
export const updateUserApi = async (userData: UpdateUserRequest): Promise<AdminUser> => {
  try {
    const token = getToken();
    if (!token) {
      throw new Error('Chưa đăng nhập');
    }

    // Use update_profile endpoint which updates the current authenticated user's profile
    const response = await apiClient.put(`${getUsersApiBaseUrl()}/update_profile`, userData);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error('Failed to update user profile');
  } catch (error: any) {
    console.error('Error updating user profile:', error);
    throw new Error(error.response?.data?.message || 'Không thể cập nhật thông tin người dùng');
  }
};

/**
 * Toggle user status (Admin only) - Lock/unlock user account
 * @param userId - User ID
 * @param isActive - True to activate, false to lock
 * @returns Updated user
 */
export const toggleUserStatusApi = async (userId: string, isActive: boolean): Promise<AdminUser> => {
  try {
    const response = await apiClient.put(`${getUsersApiBaseUrl()}/${userId}/toggle-status`, {
      is_active: isActive,
    });
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error('Failed to toggle user status');
  } catch (error: any) {
    console.error('Error toggling user status:', error);
    throw new Error(error.response?.data?.message || 'Không thể thay đổi trạng thái người dùng');
  }
};

/**
 * Top up balance for current user
 * @param amount - Amount to top up
 * @returns Updated user with new balance
 */
export const topUpBalanceApi = async (amount: number): Promise<AdminUser> => {
  try {
    const response = await apiClient.post(`${getUsersApiBaseUrl()}/balance/topup`, {
      amount: amount,
    });
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error('Failed to top up balance');
  } catch (error: any) {
    console.error('Error topping up balance:', error);
    throw new Error(error.response?.data?.message || 'Không thể nạp tiền');
  }
};