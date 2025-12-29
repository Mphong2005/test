import React from 'react';
import { HashRouter, Route, Routes, Navigate } from 'react-router-dom';

import { MainLayout } from './layouts/MainLayout';
import { AdminLayout } from './layouts/AdminLayout'; // Explicit import needed if AdminRoute doesn't wrap layout
import { ShipperLayout } from './layouts/ShipperLayout';
import { AdminRoute, PublicRoute, ShipperRoute, UserRoute } from './routes';

import { HomePage } from './page/user/HomePage';
import { LoginPage } from './page/auth/LoginPage';
import { RegisterPage } from './page/auth/RegisterPage';
import { ForgotPasswordPage } from './page/auth/ForgotPasswordPage';
import { UsersManagement } from './page/admin/UsersManagement';
import { DashboardPage } from './page/admin/DashboardPage';
import { RestaurantManagement } from './page/admin/RestaurantManagement';
import { OrderManagement } from './page/admin/OrderManagement';
import { VoucherManagement } from './page/admin/VoucherManagement'; // Import Voucher Page
import { SearchPage } from './page/user/SearchPage';
import { ProductDetailPage } from './page/user/ProductDetailPage';
import { CheckoutPage } from './page/user/CheckoutPage';
import { OrdersPage } from './page/user/OrdersPage';
import { ProfilePage } from './page/user/ProfilePage';
import { ReviewPage } from './page/user/ReviewPage';
import { AuthProvider } from './contexts/AuthContext';
import { CartProvider } from './contexts/CartContext';

// Shipper Pages
import { ShipperHomePage } from './page/shipper/ShipperHomePage';
import { ShipperHistoryPage } from './page/shipper/ShipperHistoryPage';
import { ShipperPendingPage } from './page/shipper/ShipperPendingPage';
import { ShipperProfilePage } from './page/shipper/ShipperProfilePage';

const App: React.FC = () => {
  return (
    <AuthProvider>
      <CartProvider>
        <HashRouter>
          <Routes>
            {/* Admin routes - protected */}
            <Route path="/admin" element={<Navigate to="/admin/dashboard" replace />} />

            <Route
              path="/admin/dashboard"
              element={
                <AdminRoute>
                  <DashboardPage />
                </AdminRoute>
              }
            />
            <Route
              path="/admin/users"
              element={
                <AdminRoute>
                  <UsersManagement />
                </AdminRoute>
              }
            />
            <Route
              path="/admin/restaurants"
              element={
                <AdminRoute>
                  <RestaurantManagement />
                </AdminRoute>
              }
            />
            <Route
              path="/admin/orders"
              element={
                <AdminRoute>
                  <OrderManagement />
                </AdminRoute>
              }
            />
            <Route
              path="/admin/vouchers"
              element={
                <AdminRoute>
                  <VoucherManagement />
                </AdminRoute>
              }
            />

            {/* Shipper routes - protected */}
            <Route path="/shipper" element={<Navigate to="/shipper/home" replace />} />

            <Route
              path="/shipper/home"
              element={
                <ShipperRoute>
                  <ShipperLayout>
                    <ShipperHomePage />
                  </ShipperLayout>
                </ShipperRoute>
              }
            />
            <Route
              path="/shipper/history"
              element={
                <ShipperRoute>
                  <ShipperLayout>
                    <ShipperHistoryPage />
                  </ShipperLayout>
                </ShipperRoute>
              }
            />
            <Route
              path="/shipper/pending"
              element={
                <ShipperRoute>
                  <ShipperLayout>
                    <ShipperPendingPage />
                  </ShipperLayout>
                </ShipperRoute>
              }
            />
            <Route
              path="/shipper/profile"
              element={
                <ShipperRoute>
                  <ShipperLayout>
                    <ShipperProfilePage />
                  </ShipperLayout>
                </ShipperRoute>
              }
            />

            {/* Auth routes - only for guests */}
            <Route path="/login" element={
              <PublicRoute>
                <MainLayout>
                  <LoginPage />
                </MainLayout>
              </PublicRoute>
            } />
            <Route path="/register" element={
              <PublicRoute>
                <MainLayout>
                  <RegisterPage />
                </MainLayout>
              </PublicRoute>
            } />
            <Route path="/forgot-password" element={
              <PublicRoute>
                <MainLayout>
                  <ForgotPasswordPage />
                </MainLayout>
              </PublicRoute>
            } />

            {/* Public routes */}
            <Route path="/" element={
              <MainLayout>
                <HomePage />
              </MainLayout>
            } />

            <Route path="/search" element={
              <MainLayout>
                <SearchPage />
              </MainLayout>
            } />

            <Route path="/product/:id" element={
              <MainLayout>
                <ProductDetailPage />
              </MainLayout>
            } />

            <Route path="/product/:id/reviews" element={
              <MainLayout>
                <ReviewPage />
              </MainLayout>
            } />

            <Route path="/checkout" element={
              <UserRoute>
                <MainLayout>
                  <CheckoutPage />
                </MainLayout>
              </UserRoute>
            } />

            <Route path="/orders" element={
              <UserRoute>
                <MainLayout>
                  <OrdersPage />
                </MainLayout>
              </UserRoute>
            } />

            <Route path="/profile" element={
              <UserRoute>
                <MainLayout>
                  <ProfilePage />
                </MainLayout>
              </UserRoute>
            } />

            {/* 404 */}
            <Route path="*" element={
              <MainLayout>
                <HomePage />
              </MainLayout>
            } />
          </Routes>
        </HashRouter>
      </CartProvider>
    </AuthProvider>
  );
};

export default App;
