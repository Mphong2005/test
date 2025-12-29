/**
 * Dashboard API
 * Kết nối với Backend API Dashboard
 * Chỉ chứa các HTTP calls, logic transform data nằm ở dashboardService
 */

import { getBackendBaseUrl, apiClient } from './axiosClient';
import { transformDashboardData } from '../services/dashboardService';

const DASHBOARD_BASE_URL = `${getBackendBaseUrl()}/api/dashboard`;

/**
 * Get full dashboard data (all data in one request)
 * @param year - Optional year for revenue data (defaults to current year from backend)
 */
export const getDashboardStatsApi = async (year?: number) => {
  try {
    // Get full dashboard data
    const fullResponse = await apiClient.get(`${DASHBOARD_BASE_URL}/full`);
    const fullData = fullResponse.data;

    if (!fullData.success || !fullData.data) {
      throw new Error('Invalid response format');
    }

    const dashboardData = fullData.data;

    // Debug: Log the response structure
    console.log('Dashboard API Response:', dashboardData);

    // If year is specified and different from default, get monthly revenue for that year
    let revenueData = dashboardData.monthlyRevenue?.data || dashboardData.monthly_revenue?.data || [];
    if (year) {
      try {
        const revenueResponse = await apiClient.get(`${DASHBOARD_BASE_URL}/monthly-revenue`, {
          params: { year }
        });
        if (revenueResponse.data.success && revenueResponse.data.data) {
          revenueData = revenueResponse.data.data.data || revenueResponse.data.data;
        }
      } catch (err) {
        console.warn('Could not fetch monthly revenue for year:', year, err);
        // Use default revenue data
      }
    }

    // Transform using dashboardService
    return transformDashboardData(dashboardData, revenueData);
  } catch (error: any) {
    console.error('Error fetching dashboard data:', error);
    throw new Error(error.response?.data?.message || error.message || 'Không thể tải dữ liệu dashboard');
  }
};

/**
 * Get dashboard overview stats only
 */
export const getDashboardOverviewApi = async () => {
  const response = await apiClient.get(`${DASHBOARD_BASE_URL}/overview`);
  return response.data.data;
};

/**
 * Get monthly revenue data
 */
export const getMonthlyRevenueApi = async (year?: number) => {
  const params = year ? { year } : {};
  const response = await apiClient.get(`${DASHBOARD_BASE_URL}/monthly-revenue`, { params });
  return response.data.data;
};

/**
 * Get order status distribution
 */
export const getOrderStatusDistributionApi = async () => {
  const response = await apiClient.get(`${DASHBOARD_BASE_URL}/order-status`);
  return response.data.data;
};

/**
 * Get recent activities
 */
export const getRecentActivitiesApi = async (limit: number = 10) => {
  const response = await apiClient.get(`${DASHBOARD_BASE_URL}/recent-activities`, {
    params: { limit }
  });
  return response.data.data;
};

/**
 * Get top selling items
 */
export const getTopSellingApi = async (limit: number = 10) => {
  const response = await apiClient.get(`${DASHBOARD_BASE_URL}/top-selling`, {
    params: { limit }
  });
  return response.data.data;
};

/**
 * Get top restaurants by revenue
 */
export const getTopRestaurantsApi = async (limit: number = 10) => {
  const response = await apiClient.get(`${DASHBOARD_BASE_URL}/top-restaurants`, {
    params: { limit }
  });
  return response.data.data;
};

