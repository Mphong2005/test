/**
 * Main Layout
 * Layout chung cho các trang user (có Header và Footer)
 */

import React from 'react';
import { Header } from '../components/user/Header';
import { Footer } from '../components/user/Footer';

interface MainLayoutProps {
  children: React.ReactNode;
}

export const MainLayout: React.FC<MainLayoutProps> = ({ children }) => {
  return (
    <div className="min-h-screen bg-white text-gray-800">
      <Header />
      <div className="flex min-h-screen flex-col pt-[80px]">
        <div className="flex-1">
          {children}
        </div>
        <Footer />
      </div>
    </div>
  );
};

