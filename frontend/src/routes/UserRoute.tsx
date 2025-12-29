import React, { useEffect, useState } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { isTokenValid } from '../services/authService';

interface UserRouteProps {
    children: React.ReactNode;
}

/**
 * UserRoute - Route guard cho các trang yêu cầu đăng nhập
 * Nếu user chưa đăng nhập, redirect về /login
 * 
 * Sử dụng cho: /checkout, /orders, /profile
 */
export const UserRoute: React.FC<UserRouteProps> = ({ children }) => {
    const [isAuthorized, setIsAuthorized] = useState<boolean | null>(null);
    const location = useLocation();

    useEffect(() => {
        // Check if token is valid
        if (isTokenValid()) {
            setIsAuthorized(true);
        } else {
            console.log('[UserRoute] User not logged in, redirecting to login');
            setIsAuthorized(false);
        }
    }, []);

    if (isAuthorized === null) {
        // Still checking
        return (
            <div className="min-h-screen bg-gray-50 flex items-center justify-center">
                <div className="text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#EE501C] mx-auto"></div>
                    <p className="mt-4 text-gray-600">Đang kiểm tra đăng nhập...</p>
                </div>
            </div>
        );
    }

    if (!isAuthorized) {
        // Redirect to login with return URL
        return <Navigate to="/login" state={{ from: location }} replace />;
    }

    return <>{children}</>;
};
