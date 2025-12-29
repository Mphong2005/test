/**
 * Authentication Types
 * Các types liên quan đến xác thực người dùng
 */

import { BackendRole, Gender } from './common';

// ============ API Request Types ============

// Login request
export interface LoginRequest {
  email: string;
  password: string;
}

// Register request
export interface RegisterRequest {
  fullname: string;
  email: string;
  password: string;
  phone_number?: string;
  address?: string;
  birthday?: string;
  gender?: Gender;
}

// ============ API Response Types ============

// Auth API response
export interface AuthResponse {
  data?: {
    token?: string;
    refresh_token?: string;
    user?: {
      user_id: string;
      fullname: string;
      email: string;
      role: BackendRole;
    };
  };
}

// ============ Service Result Types ============

// Token payload from JWT
export interface TokenPayload {
  user_id: string;
  email: string;
  fullname?: string;
  role?: BackendRole;
  exp?: number;
}

// Login result (from service)
export interface LoginResult {
  success: boolean;
  message: string;
  isAdmin?: boolean;
}

// Register result (from service)
export interface RegisterResult {
  success: boolean;
  message: string;
}

// Current user info
export interface CurrentUser {
  id: string;
  email: string;
  name?: string;
  role?: string;
}

// ============ Context Types ============

// Auth context type
export interface AuthContextType {
  user: CurrentUser | null;
  isAuthenticated: boolean;
  isAdmin: boolean;
  isLoading: boolean;
  login: (credentials: LoginRequest) => Promise<LoginResult>;
  register: (userData: RegisterRequest) => Promise<RegisterResult>;
  logout: () => void;
  refreshAuth: () => void;
}
