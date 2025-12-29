/**
 * Storage Utilities
 * Quản lý lưu trữ sử dụng localStorage để token được lưu giữ qua các lần reload trang
 */

// Storage Keys
export const STORAGE_KEYS = {
  TOKEN: 'token',
  REFRESH_TOKEN: 'refresh_token',
  ADMIN_NAME: 'adminName',
  ADMIN_EMAIL: 'adminEmail',
} as const;

type StorageKey = typeof STORAGE_KEYS[keyof typeof STORAGE_KEYS];

/**
 * Lấy giá trị từ localStorage
 */
export const getItem = (key: StorageKey): string | null => {
  try {
    return localStorage.getItem(key);
  } catch (error) {
    console.error(`Error getting item from localStorage: ${error}`);
    return null;
  }
};

/**
 * Lưu giá trị vào localStorage
 */
export const setItem = (key: StorageKey, value: string): void => {
  try {
    localStorage.setItem(key, value);
  } catch (error) {
    console.error(`Error setting item to localStorage: ${error}`);
  }
};

/**
 * Xóa giá trị khỏi localStorage
 */
export const removeItem = (key: StorageKey): void => {
  try {
    localStorage.removeItem(key);
  } catch (error) {
    console.error(`Error removing item from localStorage: ${error}`);
  }
};

/**
 * Xóa nhiều giá trị
 */
export const removeItems = (keys: StorageKey[]): void => {
  keys.forEach(key => removeItem(key));
};

/**
 * Xóa toàn bộ auth data
 */
export const clearAuthData = (): void => {
  removeItems([STORAGE_KEYS.TOKEN, STORAGE_KEYS.REFRESH_TOKEN, STORAGE_KEYS.ADMIN_NAME, STORAGE_KEYS.ADMIN_EMAIL]);
};

// ============ Specific Getters/Setters ============

export const getToken = (): string | null => getItem(STORAGE_KEYS.TOKEN);
export const setToken = (token: string): void => setItem(STORAGE_KEYS.TOKEN, token);

export const getRefreshToken = (): string | null => getItem(STORAGE_KEYS.REFRESH_TOKEN);
export const setRefreshToken = (refreshToken: string): void => setItem(STORAGE_KEYS.REFRESH_TOKEN, refreshToken);

export const getAdminInfo = (): { name: string | null; email: string | null } => {
  return {
    name: getItem(STORAGE_KEYS.ADMIN_NAME),
    email: getItem(STORAGE_KEYS.ADMIN_EMAIL),
  };
};

export const setAdminInfo = (name: string, email: string): void => {
  setItem(STORAGE_KEYS.ADMIN_NAME, name);
  setItem(STORAGE_KEYS.ADMIN_EMAIL, email);
};
