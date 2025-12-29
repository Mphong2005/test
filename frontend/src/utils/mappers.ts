/**
 * Mappers
 * Các hàm và constants chuyển đổi giữa Backend và Frontend formats
 */

import { Role, BackendRole, Gender, GenderVN, Status } from '../types/common';

// ============ Role Constants ============

export const ROLE_MAP: Record<BackendRole, Role> = {
  admin: 'Admin',
  user: 'User',
  shipper: 'Shipper',
} as const;

export const ROLE_MAP_REVERSE: Record<Role, BackendRole> = {
  Admin: 'admin',
  User: 'user',
  Shipper: 'shipper',
} as const;

// ============ Gender Constants ============

export const GENDER_MAP: Record<Gender, GenderVN> = {
  Male: 'Nam',
  Female: 'Nữ',
} as const;

export const GENDER_MAP_REVERSE: Record<string, Gender | undefined> = {
  'Nam': 'Male',
  'Nữ': 'Female',
  'Khác': undefined,
} as const;

// ============ Status Constants ============

export const STATUS_OPTIONS: Status[] = ['Active', 'Inactive', 'Banned'];

// ============ Role Mappers ============

/**
 * Map backend role to frontend role
 * @param backendRole - Role từ backend (admin, user, shipper)
 * @returns Frontend role (Admin, User, Shipper)
 */
export const mapRole = (backendRole: BackendRole): Role => {
  return ROLE_MAP[backendRole] || 'User';
};

/**
 * Map frontend role to backend role
 * @param role - Role từ frontend (Admin, User, Shipper)
 * @returns Backend role (admin, user, shipper)
 */
export const mapRoleToBackend = (role: string): BackendRole => {
  return ROLE_MAP_REVERSE[role as Role] || 'user';
};

// ============ Gender Mappers ============

/**
 * Map backend gender to Vietnamese display
 * @param gender - Gender từ backend (Male, Female)
 * @returns Vietnamese gender (Nam, Nữ, Khác)
 */
export const mapGender = (gender: Gender | null | undefined): GenderVN => {
  if (!gender) return 'Khác';
  return GENDER_MAP[gender] || 'Khác';
};

/**
 * Map Vietnamese gender to backend format
 * @param gender - Vietnamese gender (Nam, Nữ, Khác)
 * @returns Backend gender (Male, Female, undefined)
 */
export const mapGenderToBackend = (gender: string): Gender | undefined => {
  return GENDER_MAP_REVERSE[gender];
};
