/**
 * Review API
 * Kết nối UI với Backend API cho Đánh giá & Nhận xét
 * Chỉ chứa các HTTP calls, logic transform data nằm ở reviewService
 */

import { Review } from '../types/common';
import { getBackendBaseUrl, apiClient } from './axiosClient';
import { transformReviews, prepareReviewSubmission } from '../services/reviewService';

/**
 * Get reviews by food ID from backend API
 */
export const getReviewsByFoodIdApi = async (foodId: string): Promise<Review[]> => {
  const backendUrl = getBackendBaseUrl();
  const response = await fetch(`${backendUrl}/api/reviews/food/${encodeURIComponent(foodId)}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const data = await response.json();

  if (data && data.success && data.data && Array.isArray(data.data)) {
    return transformReviews(data.data, foodId);
  }

  return [];
};

/**
 * Submit review for an order
 * Calls backend API: POST /api/reviews/user
 * Body: { orderId, rating, comment }
 */
export const submitReviewApi = async (data: {
  orderId: string;
  rating: number;
  comment?: string;
}): Promise<any> => {
  try {
    const submissionData = prepareReviewSubmission(data);
    console.log('[submitReviewApi] Sending:', submissionData);

    const response = await apiClient.post(`${getBackendBaseUrl()}/api/reviews/user`, submissionData);

    console.log('[submitReviewApi] Response:', response.data);
    if (response.data.success) {
      return response.data.data;
    }
    throw new Error(response.data.message || 'Không thể gửi đánh giá');
  } catch (error: any) {
    console.error('[submitReviewApi] Error:', error.response?.data || error.message);
    const errorMessage = error.response?.data?.message || 'Không thể gửi đánh giá';
    throw new Error(errorMessage);
  }
};