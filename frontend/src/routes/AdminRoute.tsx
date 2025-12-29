import React, { useEffect, useState } from 'react';
import { Navigate } from 'react-router-dom';
import { isTokenValid, isAdmin, logout } from '../services/authService';

interface AdminRouteProps {
  children: React.ReactNode;
}

export const AdminRoute: React.FC<AdminRouteProps> = ({ children }) => {
  const [isAuthorized, setIsAuthorized] = useState<boolean | null>(null);

  useEffect(() => {
    // Check if token is valid
    if (!isTokenValid()) {
      console.log('[AdminRoute] Token invalid or expired');
      logout();
      setIsAuthorized(false);
      return;
    }

    // Check if user has admin role
    if (isAdmin()) {
      setIsAuthorized(true);
    } else {
      console.log('[AdminRoute] User does not have admin role');
      setIsAuthorized(false);
    }
  }, []);

  if (isAuthorized === null) {
    // Still checking
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#EE501C] mx-auto"></div>
          <p className="mt-4 text-gray-600">Đang kiểm tra quyền truy cập...</p>
        </div>
      </div>
    );
  }

  if (!isAuthorized) {
    return <Navigate to="/" replace />;
  }

  return <>{children}</>;
};
