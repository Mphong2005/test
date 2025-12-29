
/**
 * Product API
 * Kết nối UI với Backend API
 * Chỉ chứa các HTTP calls, logic transform data nằm ở productService
 */

import { FoodItem } from '../types/common';
import { getBackendBaseUrl } from './axiosClient';
import {
  transformFoodItem,
  transformFoodItems,
  transformCategories,
  transformPromotions,
  transformToSuggestions
} from '../services/productService';

/**
 * Get all foods from backend API
 */
export const getFoodsApi = async (): Promise<FoodItem[]> => {
  const backendUrl = getBackendBaseUrl();
  const response = await fetch(`${backendUrl}/api/restaurants/foods`, {
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
    return transformFoodItems(data.data);
  }

  throw new Error('Invalid API response');
};

/**
 * Get food by ID from backend API
 */
export const getFoodByIdApi = async (id: string): Promise<FoodItem | undefined> => {
  const backendUrl = getBackendBaseUrl();
  const response = await fetch(`${backendUrl}/api/restaurants/foods/${encodeURIComponent(id)}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    if (response.status === 404) {
      return undefined;
    }
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const data = await response.json();

  if (data && data.success && data.data) {
    return transformFoodItem(data.data);
  }

  return undefined;
};

/**
 * Get categories from backend API (based on restaurants menu)
 */
export const getCategoriesApi = async () => {
  const backendUrl = getBackendBaseUrl();
  const response = await fetch(`${backendUrl}/api/restaurants/categories`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const data = await response.json();
  console.log('Categories API response:', data);

  if (data && data.success && data.data && Array.isArray(data.data) && data.data.length > 0) {
    return transformCategories(data.data);
  }

  throw new Error('Invalid API response');
};

/**
 * Get promotions from backend API (based on restaurants with reviews)
 */
export const getPromotionsApi = async () => {
  const backendUrl = getBackendBaseUrl();
  const response = await fetch(`${backendUrl}/api/restaurants/promotions?limit=8`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const data = await response.json();
  console.log('Promotions API response:', data);

  if (data && data.success && data.data && Array.isArray(data.data) && data.data.length > 0) {
    return transformPromotions(data.data, 8);
  }

  throw new Error('Invalid API response');
};

// Suggestions API - returns categories as suggestions
export const getSuggestionsApi = async () => {
  try {
    const categories = await getCategoriesApi();
    return transformToSuggestions(categories, 8);
  } catch {
    return [];
  }
};

// Search History APIs - using localStorage as temporary solution
export const getSearchHistoryApi = async (): Promise<string[]> => {
  try {
    const history = localStorage.getItem('search_history');
    return history ? JSON.parse(history) : [];
  } catch {
    return [];
  }
};

export const addSearchHistoryApi = async (term: string): Promise<string[]> => {
  try {
    const history = await getSearchHistoryApi();
    const updatedHistory = [term, ...history.filter(item => item !== term)].slice(0, 10);
    localStorage.setItem('search_history', JSON.stringify(updatedHistory));
    return updatedHistory;
  } catch {
    return [];
  }
};

export const clearSearchHistoryApi = async (): Promise<void> => {
  try {
    localStorage.removeItem('search_history');
  } catch {
    // Ignore errors
  }
};
