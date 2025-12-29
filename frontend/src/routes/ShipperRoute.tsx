
import React, { useEffect, useState } from 'react';
import { Navigate } from 'react-router-dom';
import { isTokenValid, logout, getTokenPayload } from '../services/authService';

interface ShipperRouteProps {
  children: React.ReactNode;
}

export const ShipperRoute: React.FC<ShipperRouteProps> = ({ children }) => {
  const [isAuthorized, setIsAuthorized] = useState<boolean | null>(null);

  useEffect(() => {
    if (!isTokenValid()) {
      logout();
      setIsAuthorized(false);
      return;
    }

    const payload = getTokenPayload();
    // Allow 'shipper' role or 'admin' (for testing)
    if (payload?.role === 'shipper' || payload?.role === 'admin') {
      setIsAuthorized(true);
    } else {
      setIsAuthorized(false);
    }
  }, []);

  if (isAuthorized === null) {
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
