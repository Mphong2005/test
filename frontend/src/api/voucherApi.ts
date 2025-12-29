/**
 * Voucher API
 * Kết nối với Backend API Voucher
 * Chỉ chứa các HTTP calls, logic transform data nằm ở voucherService
 */

import { getBackendBaseUrl, apiClient } from './axiosClient';
import { Voucher } from '../types/common';
import {
  transformVoucher,
  transformVouchers,
  transformVoucherForUser,
  transformVoucherToBackend,
  isVoucherExpired
} from '../services/voucherService';

const VOUCHER_BASE_URL = `${getBackendBaseUrl()}/api/vouchers`;

/**
 * Get all vouchers (Admin endpoint - returns all vouchers)
 * For admin management page, always use admin endpoint
 */
export const getVouchersApi = async (): Promise<Voucher[]> => {
  try {
    const response = await apiClient.get(`${VOUCHER_BASE_URL}/admin`);

    if (response.data && response.data.success && response.data.data) {
      const vouchers = transformVouchers(response.data.data);
      console.log(`Loaded ${vouchers.length} vouchers from backend`);
      return vouchers;
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching vouchers:', error);
    return [];
  }
};

/**
 * Get available vouchers for user (User endpoint - returns only active vouchers)
 */
export const getAvailableVouchersForUserApi = async (restaurantId?: string): Promise<Voucher[]> => {
  try {
    const params = restaurantId ? { restaurantId } : {};
    const response = await apiClient.get(`${VOUCHER_BASE_URL}/user/available`, { params });

    if (response.data && response.data.success && response.data.data) {
      const vouchers = response.data.data.map((v: any) => transformVoucherForUser(v));
      console.log(`Loaded ${vouchers.length} available vouchers for user`);
      return vouchers;
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching available vouchers for user:', error);
    console.error('Error details:', {
      message: error.message,
      response: error.response?.data,
      status: error.response?.status,
    });
    return [];
  }
};

/**
 * Create voucher (Admin only)
 */
export const createVoucherApi = async (voucherData: Omit<Voucher, 'id'>): Promise<Voucher> => {
  try {
    const backendData = transformVoucherToBackend(voucherData);
    // Set defaults for create
    if (!backendData.start_date) backendData.start_date = new Date().toISOString();
    if (!backendData.end_date) backendData.end_date = new Date().toISOString();

    const response = await apiClient.post(`${VOUCHER_BASE_URL}/admin`, backendData);
    if (response.data.success && response.data.data) {
      return transformVoucher(response.data.data);
    }
    throw new Error('Failed to create voucher');
  } catch (error: any) {
    console.error('Error creating voucher:', error);
    throw new Error(error.response?.data?.message || 'Không thể tạo voucher');
  }
};

/**
 * Update voucher (Admin only)
 */
export const updateVoucherApi = async (id: string, updates: Partial<Voucher>): Promise<Voucher | null> => {
  try {
    const backendData = transformVoucherToBackend(updates);

    const response = await apiClient.put(`${VOUCHER_BASE_URL}/admin/${id}`, backendData);
    if (response.data.success && response.data.data) {
      return transformVoucher(response.data.data);
    }
    throw new Error('Failed to update voucher');
  } catch (error: any) {
    console.error('Error updating voucher:', error);
    throw new Error(error.response?.data?.message || 'Không thể cập nhật voucher');
  }
};

/**
 * Delete voucher (Admin only)
 */
export const deleteVoucherApi = async (id: string): Promise<void> => {
  try {
    const response = await apiClient.delete(`${VOUCHER_BASE_URL}/admin/${id}`);
    if (response.data.success) {
      return;
    }
    throw new Error('Failed to delete voucher');
  } catch (error: any) {
    console.error('Error deleting voucher:', error);
    throw new Error(error.response?.data?.message || 'Không thể xóa voucher');
  }
};

/**
 * Get available vouchers (Admin - for filtering)
 */
export const getAvailableVouchersApi = async (restaurantId?: string): Promise<Voucher[]> => {
  try {
    const params = restaurantId ? { restaurantId } : {};
    const response = await apiClient.get(`${VOUCHER_BASE_URL}/admin/available`, { params });
    if (response.data.success && response.data.data) {
      return transformVouchers(response.data.data);
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching available vouchers:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải voucher khả dụng');
  }
};

/**
 * Get expired vouchers for user (User endpoint - returns expired/inactive vouchers)
 */
export const getExpiredVouchersForUserApi = async (restaurantId?: string): Promise<Voucher[]> => {
  try {
    const params = restaurantId ? { restaurantId } : {};
    const response = await apiClient.get(`${VOUCHER_BASE_URL}/user/expired`, { params });

    if (response.data && response.data.success && response.data.data) {
      const vouchers = transformVouchers(response.data.data);
      console.log(`Loaded ${vouchers.length} expired vouchers for user`);
      return vouchers;
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching expired vouchers for user:', error);
    console.error('Error details:', {
      message: error.message,
      response: error.response?.data,
      status: error.response?.status,
    });
    return [];
  }
};

/**
 * Get expired vouchers (Admin)
 */
export const getExpiredVouchersApi = async (): Promise<Voucher[]> => {
  try {
    const response = await apiClient.get(`${VOUCHER_BASE_URL}/admin/expired`);
    if (response.data.success && response.data.data) {
      return response.data.data.map((voucher: any) => ({
        ...transformVoucher(voucher),
        status: 'Expired' as const,
        isExpired: true,
      }));
    }
    return [];
  } catch (error: any) {
    console.error('Error fetching expired vouchers:', error);
    throw new Error(error.response?.data?.message || 'Không thể tải voucher đã hết hạn');
  }
};

