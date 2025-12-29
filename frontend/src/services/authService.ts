/**
 * Authentication Service
 * File này chứa các nghiệp vụ liên quan đến xác thực người dùng
 */

import { jwtDecode } from 'jwt-decode';
import { loginApi, registerApi, refreshTokenApi, LoginRequest, RegisterRequest } from '../api/authApi';
import { buildNetworkErrorMessage } from '../api/axiosClient';
import { TokenPayload, LoginResult, RegisterResult, CurrentUser } from '../types/auth';
import {
  getToken,
  setToken,
  getRefreshToken,
  setRefreshToken,
  clearAuthData,
  getAdminInfo as getStoredAdminInfo,
  setAdminInfo
} from '../utils';

// Re-export types for backward compatibility
export type { TokenPayload, LoginResult, RegisterResult, CurrentUser } from '../types/auth';
// Re-export setAdminInfo for external use
export { setAdminInfo } from '../utils';

/**
 * Helper để tạo Fake JWT cho mục đích test (Bypass Backend)
 */
const createMockToken = (payload: any) => {
  // Fix: Encode Unicode strings (like Vietnamese names) before btoa
  const encode = (str: string) => {
    return btoa(encodeURIComponent(str).replace(/%([0-9A-F]{2})/g,
      function toSolidBytes(match, p1) {
        return String.fromCharCode(parseInt(p1, 16));
      }));
  };

  const header = encode(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = encode(JSON.stringify(payload));
  const signature = "mock_signature";
  return `${header}.${body}.${signature}`;
};

/**
 * Đăng nhập và lưu token
 * @param credentials - Email và mật khẩu
 * @returns LoginResult
 */
export const login = async (credentials: LoginRequest): Promise<LoginResult> => {
  // --- MOCK LOGIN FOR TESTING ---
  // Cho phép đăng nhập nhanh không cần backend
  if (credentials.password === '123456') {
    let mockPayload = null;

    if (credentials.email === 'admin@gmail.com') {
      mockPayload = {
        user_id: 'usr-admin',
        email: 'admin@gmail.com',
        fullname: 'Admin Test',
        role: 'admin', // BackendRole
        exp: Math.floor(Date.now() / 1000) + (60 * 60 * 24) // 24h
      };
    } else if (credentials.email === 'user@gmail.com') {
      mockPayload = {
        user_id: 'usr-1', // Match INITIAL_DATA user
        email: 'user@gmail.com',
        fullname: 'Nguyễn Văn A',
        role: 'user', // BackendRole
        exp: Math.floor(Date.now() / 1000) + (60 * 60 * 24) // 24h
      };
    } else if (credentials.email === 'shipper@food.com') {
      mockPayload = {
        user_id: 'usr-shipper',
        email: 'shipper@food.com',
        fullname: 'Trần Văn Shipper',
        role: 'shipper', // BackendRole
        exp: Math.floor(Date.now() / 1000) + (60 * 60 * 24) // 24h
      };
    }

    if (mockPayload) {
      const fakeToken = createMockToken(mockPayload);
      const fakeRefreshToken = createMockToken({ ...mockPayload, type: 'refresh' });
      setToken(fakeToken);
      setRefreshToken(fakeRefreshToken);
      return {
        success: true,
        message: 'Đăng nhập Test thành công!',
        isAdmin: mockPayload.role === 'admin',
      };
    }
  }
  // -----------------------------
  try {
    const data = await loginApi(credentials);

    const token = data?.data?.token;
    const refreshToken = data?.data?.refresh_token;

    if (token) {
      setToken(token);
      if (refreshToken) {
        setRefreshToken(refreshToken);
      }

      // Check if user is admin
      try {
        const decoded = jwtDecode<TokenPayload>(token);
        return {
          success: true,
          message: 'Đăng nhập thành công!',
          isAdmin: decoded.role === 'admin',
        };
      } catch (error) {
        console.error('Error decoding token:', error);
      }
    }

    return {
      success: true,
      message: 'Đăng nhập thành công!',
    };
  } catch (error) {
    return {
      success: false,
      message: buildNetworkErrorMessage(error),
    };
  }
};

/**
 * Đăng ký tài khoản mới
 * @param userData - Thông tin đăng ký
 * @returns RegisterResult
 */
export const register = async (userData: RegisterRequest): Promise<RegisterResult> => {
  try {
    const data = await registerApi(userData);

    const token = data?.data?.token;
    const refreshToken = data?.data?.refresh_token;

    if (token) {
      setToken(token);
      if (refreshToken) {
        setRefreshToken(refreshToken);
      }
    }

    return {
      success: true,
      message: 'Đăng ký thành công! Bạn có thể đăng nhập ngay bây giờ.',
    };
  } catch (error) {
    return {
      success: false,
      message: buildNetworkErrorMessage(error),
    };
  }
};

/**
 * Đăng xuất - xóa token và auth data
 */
export const logout = (): void => {
  clearAuthData();
};

/**
 * Lấy thông tin từ token
 * @returns TokenPayload hoặc null
 */
export const getTokenPayload = (): TokenPayload | null => {
  const token = getToken();
  if (!token) return null;

  try {
    return jwtDecode<TokenPayload>(token);
  } catch (error) {
    console.error('Error decoding token:', error);
    return null;
  }
};

/**
 * Kiểm tra token có hợp lệ không
 * @returns boolean
 */
export const isTokenValid = (): boolean => {
  const payload = getTokenPayload();
  if (!payload) return false;

  // Check if token is expired
  if (payload.exp && payload.exp * 1000 < Date.now()) {
    logout();
    return false;
  }

  return true;
};

/**
 * Kiểm tra user có phải admin không
 * @returns boolean
 */
export const isAdmin = (): boolean => {
  const payload = getTokenPayload();
  return payload?.role === 'admin';
};

/**
 * Lấy thông tin user hiện tại từ token
 * @returns User info hoặc null
 */
export const getCurrentUser = (): CurrentUser | null => {
  const payload = getTokenPayload();
  if (!payload) return null;

  return {
    id: payload.user_id,
    email: payload.email,
    name: payload.fullname,
    role: payload.role,
  };
};

/**
 * Refresh access token bằng refresh token
 * @returns Promise<boolean> - true nếu refresh thành công
 */
export const refreshAccessToken = async (): Promise<boolean> => {
  const refreshToken = getRefreshToken();
  if (!refreshToken) {
    return false;
  }

  try {
    const data = await refreshTokenApi(refreshToken);
    const newToken = data?.data?.token;

    if (newToken) {
      setToken(newToken);
      return true;
    }

    return false;
  } catch (error) {
    console.error('Error refreshing token:', error);
    // Nếu refresh token cũng hết hạn, xóa tất cả và yêu cầu đăng nhập lại
    clearAuthData();
    return false;
  }
};

/**
 * Lấy thông tin admin từ storage hoặc token
 * @returns Admin info { name, email }
 */
export const getAdminInfo = (): { name: string; email: string } => {
  const storedInfo = getStoredAdminInfo();

  if (storedInfo.name && storedInfo.email) {
    return { name: storedInfo.name, email: storedInfo.email };
  }

  // Fallback to token
  const user = getCurrentUser();
  return {
    name: user?.name || 'Admin',
    email: user?.email || 'admin@food.com',
  };
};

/**
 * Kiểm tra user đã đăng nhập chưa (có token trong storage)
 * @returns boolean
 */
export const isAuthenticated = (): boolean => {
  const token = getToken();
  return !!token;
};

