/**
 * useAuth Hook
 * Hook xử lý logic xác thực và phân quyền
 */

import { useState, useEffect, useCallback } from 'react';
import { 
  getTokenPayload, 
  isTokenValid, 
  logout as authLogout,
  getCurrentUser 
} from '../services/authService';
import { setAdminInfo } from '../utils';
import { TokenPayload, CurrentUser } from '../types/auth';

interface UseAuthReturn {
  isAuthenticated: boolean;
  isAdmin: boolean;
  user: CurrentUser | null;
  tokenPayload: TokenPayload | null;
  loading: boolean;
  error: string | null;
  checkAuth: () => boolean;
  checkAdminAccess: () => { valid: boolean; error?: string };
  logout: () => void;
  saveAdminInfo: () => void;
}

export const useAuth = (): UseAuthReturn => {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isAdmin, setIsAdmin] = useState(false);
  const [user, setUser] = useState<CurrentUser | null>(null);
  const [tokenPayload, setTokenPayload] = useState<TokenPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  /**
   * Kiểm tra token có hợp lệ không
   */
  const checkAuth = useCallback((): boolean => {
    const valid = isTokenValid();
    setIsAuthenticated(valid);
    return valid;
  }, []);

  /**
   * Kiểm tra quyền admin
   */
  const checkAdminAccess = useCallback((): { valid: boolean; error?: string } => {
    if (!isTokenValid()) {
      return { 
        valid: false, 
        error: 'Chưa đăng nhập. Vui lòng đăng nhập lại.' 
      };
    }

    const payload = getTokenPayload();
    if (payload?.role !== 'admin') {
      return { 
        valid: false, 
        error: 'Bạn không có quyền truy cập. Vui lòng đăng nhập lại với tài khoản admin.' 
      };
    }

    return { valid: true };
  }, []);

  /**
   * Lưu thông tin admin vào localStorage (để hiển thị ở Sidebar)
   */
  const saveAdminInfo = useCallback(() => {
    const payload = getTokenPayload();
    if (payload?.fullname && payload?.email) {
      setAdminInfo(payload.fullname, payload.email);
    }
  }, []);

  /**
   * Đăng xuất
   */
  const logout = useCallback(() => {
    authLogout();
    setIsAuthenticated(false);
    setIsAdmin(false);
    setUser(null);
    setTokenPayload(null);
  }, []);

  /**
   * Load auth state khi mount
   */
  useEffect(() => {
    const initAuth = () => {
      setLoading(true);
      setError(null);

      try {
        const valid = isTokenValid();
        setIsAuthenticated(valid);

        if (valid) {
          const payload = getTokenPayload();
          setTokenPayload(payload);
          setIsAdmin(payload?.role === 'admin');
          setUser(getCurrentUser());
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Lỗi kiểm tra xác thực');
      } finally {
        setLoading(false);
      }
    };

    initAuth();
  }, []);

  return {
    isAuthenticated,
    isAdmin,
    user,
    tokenPayload,
    loading,
    error,
    checkAuth,
    checkAdminAccess,
    logout,
    saveAdminInfo,
  };
};
