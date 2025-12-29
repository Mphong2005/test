import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Sidebar } from '../components/admin/Sidebar';
import { useAuthContext } from '../contexts/AuthContext';
import { formatDateVN } from '../utils';

interface AdminLayoutProps {
  children: React.ReactNode;
  title: string;
}

export const AdminLayout: React.FC<AdminLayoutProps> = ({ children, title }) => {
  const navigate = useNavigate();
  const { logout } = useAuthContext();

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  return (
    <div className="min-h-screen bg-gray-50 font-sans text-gray-900">
      <Sidebar onLogout={handleLogout} />
      
      {/* Main Content Layout */}
      <div className="md:ml-64 transition-all duration-300 min-h-screen">
         {/* Top Header */}
        <header className="sticky top-0 z-10 bg-white/80 backdrop-blur-md border-b border-gray-200 px-8 py-4 flex items-center justify-between">
          <h1 className="text-2xl font-black text-[#EE501C]">{title}</h1>
          <div className="flex items-center space-x-4">
             <div className="hidden md:flex items-center text-sm font-medium text-gray-500">
                <span className="mr-2">Hôm nay:</span>
                <span className="text-[#EE501C] font-bold">{formatDateVN(new Date())}</span>
             </div>
          </div>
        </header>

        {/* Page Content */}
        <main className="p-8 max-w-[1600px] mx-auto">
          {children}
        </main>
      </div>
    </div>
  );
};