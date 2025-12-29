import React from 'react';
import { Navigate } from 'react-router-dom';
import { isTokenValid } from '../services/authService';

interface PublicRouteProps {
  children: React.ReactNode;
  redirectTo?: string;
}

/**
 * PublicRoute - Route guard cho các trang dành cho guest
 * Nếu user đã đăng nhập, redirect về trang chủ (hoặc trang chỉ định)
 * 
 * Sử dụng cho: /login, /register, /forgot-password
 */
export const PublicRoute: React.FC<PublicRouteProps> = ({ 
  children, 
  redirectTo = '/' 
}) => {
  // Kiểm tra user đã đăng nhập chưa
  const isLoggedIn = isTokenValid();

  if (isLoggedIn) {
    // Đã đăng nhập → redirect về trang chủ
    console.log('[PublicRoute] User already logged in, redirecting...');
    return <Navigate to={redirectTo} replace />;
  }

  // Chưa đăng nhập → cho phép truy cập
  return <>{children}</>;
};
