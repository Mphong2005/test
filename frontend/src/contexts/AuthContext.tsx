/**
 * Auth Context
 * Context để quản lý trạng thái đăng nhập toàn ứng dụng
 */

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { 
  login as authLogin, 
  register as authRegister, 
  logout as authLogout,
  getCurrentUser,
  isTokenValid,
  isAdmin as checkIsAdmin,
} from '../services/authService';
import { 
  LoginRequest, 
  RegisterRequest, 
  CurrentUser, 
  AuthContextType 
} from '@/types/auth';

// Create context
const AuthContext = createContext<AuthContextType | undefined>(undefined);

// Provider component
export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<CurrentUser | null>(null);
  const [isAdmin, setIsAdmin] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  // Refresh auth state from token
  const refreshAuth = useCallback(() => {
    if (isTokenValid()) {
      const currentUser = getCurrentUser();
      setUser(currentUser);
      setIsAdmin(checkIsAdmin());
    } else {
      setUser(null);
      setIsAdmin(false);
    }
    setIsLoading(false);
  }, []);

  // Initialize auth state on mount
  useEffect(() => {
    refreshAuth();
  }, [refreshAuth]);

  // Login handler
  const login = async (credentials: LoginRequest) => {
    const result = await authLogin(credentials);
    if (result.success) {
      refreshAuth();
    }
    return result;
  };

  // Register handler
  const register = async (userData: RegisterRequest) => {
    const result = await authRegister(userData);
    if (result.success) {
      refreshAuth();
    }
    return result;
  };

  // Logout handler
  const logout = () => {
    authLogout();
    setUser(null);
    setIsAdmin(false);
  };

  const value: AuthContextType = {
    user,
    isAuthenticated: !!user,
    isAdmin,
    isLoading,
    login,
    register,
    logout,
    refreshAuth,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};

// Hook to use auth context
export const useAuthContext = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuthContext must be used within an AuthProvider');
  }
  return context;
};
