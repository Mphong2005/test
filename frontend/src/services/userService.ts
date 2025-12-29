/**
 * User Service
 * File này chứa các nghiệp vụ liên quan đến quản lý người dùng
 */

import {
  getAllUsersApi,
  deleteUserApi,
  updateUserRoleApi,
  createUserApi,
  toggleUserStatusApi,
  AdminUser,
  CreateUserRequest
} from '../api/userApi';
import { buildNetworkErrorMessage } from '../api/axiosClient';
import { User } from '../types/user';
import { mapRole, mapGender, mapRoleToBackend, mapGenderToBackend, formatDateISO } from '../utils';

/**
 * Map backend user format to frontend User type
 * @param backendUser - User data từ backend
 * @returns User object cho frontend
 */
export const mapBackendUserToFrontend = (backendUser: AdminUser | Record<string, unknown>): User => {
  // Handle both formats: user_id or _id
  const userId = (backendUser as AdminUser).user_id ||
    (backendUser as Record<string, unknown>)._id ||
    (backendUser as Record<string, unknown>).id;

  const user = backendUser as AdminUser;

  // Map status from backend is_active field
  let status: 'Active' | 'Inactive' | 'Banned' = 'Active';
  if ('is_active' in backendUser) {
    status = (backendUser as any).is_active ? 'Active' : 'Banned';
  } else if ('status' in backendUser) {
    status = (backendUser as any).status as 'Active' | 'Inactive' | 'Banned';
  }

  return {
    id: userId as string,
    name: user.fullname || (backendUser as Record<string, unknown>).name as string || '',
    email: user.email || '',
    phone: user.phone_number || (backendUser as Record<string, unknown>).phone as string || '',
    role: mapRole(user.role),
    status: status,
    joinDate: formatDateISO(user.created_at) || new Date().toISOString().split('T')[0],
    gender: mapGender(user.gender),
    dob: formatDateISO(user.birthday) || undefined,
    address: user.address || (backendUser as Record<string, unknown>).address as string || undefined,
  };
};

/**
 * Lấy danh sách tất cả users và map sang frontend format
 * @returns Promise<User[]>
 */
export const getAllUsers = async (): Promise<User[]> => {
  const backendUsers = await getAllUsersApi();
  return backendUsers.map(mapBackendUserToFrontend);
};

/**
 * Xóa user theo ID
 * @param userId - User ID
 */
export const deleteUser = async (userId: string): Promise<void> => {
  await deleteUserApi(userId);
};

/**
 * Cập nhật role của user
 * @param userId - User ID
 * @param role - New role (frontend format)
 * @returns Updated user
 */
export const updateUserRole = async (userId: string, role: string): Promise<User> => {
  const backendRole = mapRoleToBackend(role);
  const updatedUser = await updateUserRoleApi(userId, backendRole);
  return mapBackendUserToFrontend(updatedUser);
};

/**
 * Tạo user mới
 * @param userData - User data (frontend format)
 * @returns Created user
 */
export const createUser = async (userData: {
  name: string;
  email: string;
  password: string;
  phone: string;
  dob?: string;
  gender?: string;
  role?: string;
}): Promise<User> => {
  const createRequest: CreateUserRequest = {
    fullname: userData.name,
    email: userData.email,
    password: userData.password,
    phone_number: userData.phone,
    birthday: userData.dob ? new Date(userData.dob).toISOString() : undefined,
    gender: userData.gender ? mapGenderToBackend(userData.gender) : undefined,
    role: userData.role ? mapRoleToBackend(userData.role) : 'user',
  };

  const createdUser = await createUserApi(createRequest);
  return mapBackendUserToFrontend(createdUser);
};

/**
 * Toggle user status (lock/unlock account)
 * @param userId - User ID
 * @param isActive - True to activate, false to lock
 * @returns Updated user
 */
export const toggleUserStatus = async (userId: string, isActive: boolean): Promise<User> => {
  const updatedUser = await toggleUserStatusApi(userId, isActive);
  return mapBackendUserToFrontend(updatedUser);
};

/**
 * Transform user profile data from backend
 * Ensures consistent field names (phone_number normalization)
 */
export const transformUserProfile = (userData: any): AdminUser => {
  return {
    ...userData,
    phone_number: userData.phone_number || userData.phone || '',
  };
};

/**
 * Build error message for user operations
 */
export { buildNetworkErrorMessage };
