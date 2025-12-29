/**
 * Authentication API
 * File này chứa các API endpoints liên quan đến xác thực người dùng
 */

import { getAuthApiBaseUrl, requestJson } from './axiosClient';
import { LoginRequest, RegisterRequest, AuthResponse } from '@/types/auth';

// Re-export types for backward compatibility
export type { LoginRequest, RegisterRequest, AuthResponse } from '@/types/auth';

/**
 * Login API
 * @param credentials - Email and password
 * @returns AuthResponse with token
 */
export const loginApi = async (credentials: LoginRequest): Promise<AuthResponse> => {
  return requestJson<AuthResponse>(
    `${getAuthApiBaseUrl()}/login`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(credentials),
    }
  );
};

/**
 * Register API
 * @param userData - User registration data
 * @returns AuthResponse with token and user info
 */
export const registerApi = async (userData: RegisterRequest): Promise<AuthResponse> => {
  return requestJson<AuthResponse>(
    `${getAuthApiBaseUrl()}/register`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(userData),
    }
  );
};

/**
 * Refresh Token API
 * @param refreshToken - Refresh token
 * @returns AuthResponse with new access token
 */
export const refreshTokenApi = async (refreshToken: string): Promise<AuthResponse> => {
  return requestJson<AuthResponse>(
    `${getAuthApiBaseUrl()}/refresh`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: refreshToken }),
    }
  );
};

/**
 * Forgot Password API (placeholder - cần backend hỗ trợ)
 * @param email - User email
 * @returns Promise<void>
 */
export const forgotPasswordApi = async (email: string): Promise<void> => {
  // TODO: Implement when backend provides the endpoint
  console.log('Forgot password request for:', email);
  throw new Error('Tính năng này chưa được hỗ trợ bởi backend');
};
