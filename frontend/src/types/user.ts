/**
 * User Types
 * Các types liên quan đến người dùng
 */

import { Role, BackendRole, Status, Gender, GenderVN } from './common';

// ============ Frontend User Model ============

// Frontend User model (for UI display)
export interface User {
  id: string;
  name: string;
  email: string;
  phone: string;
  role: Role;
  status: Status;
  joinDate: string;
  avatarUrl?: string;
  gender?: GenderVN;
  dob?: string;
  address?: string;
}

// ============ Backend User Model ============
export interface BackendUser {
  user_id: string;
  fullname: string;
  email: string;
  phone_number: string | null;
  birthday: string | null;
  gender: Gender | null;
  created_at: string;
  role: BackendRole;
  is_active?: boolean; // Trạng thái tài khoản (true: hoạt động, false: bị khóa)
  // Extended fields for Profile unification
  address?: string;
  balance?: number;
  rank?: string; // e.g., 'Thành viên Bạc'
  avatar?: string;
}

// Request types for API calls
export interface CreateUserRequest {
  fullname: string;
  email: string;
  password: string;
  phone_number: string;
  birthday?: string;
  gender?: Gender;
  role?: BackendRole;
}

export interface UpdateUserRequest {
  fullname?: string;
  email?: string;
  phone_number?: string;
  birthday?: string;
  gender?: Gender;
  address?: string; // Added
}

// User profile
export interface APIUserProfile {
  id: string;
  fullname: string;
  email: string;
  phone: string;
  birthday?: string;
  gender?: Gender;
  avatar?: string;
  role: BackendRole;
  created_at: string;
  address?: string;
  balance?: number;
}
