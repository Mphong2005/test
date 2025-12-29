
import React, { useState } from 'react';
import { LayoutDashboard, Users, Store, ShoppingBag, LogOut, Ticket, Menu, X } from 'lucide-react';
import { useLocation, useNavigate } from 'react-router-dom';
import { getAdminInfo } from '../../services/authService';

interface SidebarProps {
  onLogout: () => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ onLogout }) => {
  const adminInfo = getAdminInfo();
  const location = useLocation();
  const navigate = useNavigate();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  
  // Determine active tab based on current path
  const currentPath = location.pathname;
  
  const menuItems = [
    { id: 'dashboard', path: '/admin/dashboard', label: 'Dashboard', icon: <LayoutDashboard size={20} /> },
    { id: 'users', path: '/admin/users', label: 'Quản lý Người dùng', icon: <Users size={20} /> },
    { id: 'restaurants', path: '/admin/restaurants', label: 'Quản lý Nhà hàng', icon: <Store size={20} /> },
    { id: 'orders', path: '/admin/orders', label: 'Quản lý Đơn hàng', icon: <ShoppingBag size={20} /> },
    { id: 'vouchers', path: '/admin/vouchers', label: 'Quản lý Voucher', icon: <Ticket size={20} /> },
  ];

  const isActive = (path: string) => currentPath.startsWith(path);

  const handleMenuClick = (path: string) => {
    navigate(path);
    setMobileMenuOpen(false);
  };

  const MenuContent = () => (
    <>
      {/* Brand */}
      <div className="h-16 flex items-center px-6 border-b border-gray-100">
        <div className="w-8 h-8 mr-3">
          <svg 
            xmlns="http://www.w3.org/2000/svg" 
            viewBox="0 0 48 48" 
            fill="none"
            className="w-full h-full object-contain"
          >
            <circle cx="24" cy="24" r="24" fill="#EE501C"/>
            <g stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" fill="none" transform="translate(12, 10)">
              <path d="M3 2v7a4 4 0 0 0 4 4v13"/>
              <path d="M3 2h4"/>
              <path d="M5 2v7"/>
              <path d="M21 15V2a5 5 0 0 0-5 5v6c0 1.1.9 2 2 2h3v13"/>
            </g>
          </svg>
        </div>
        <span className="text-xl font-bold text-[#EE501C]">FoodDelivery</span>
      </div>

      {/* Admin Profile Small */}
      <div className="p-6 pb-2">
        <div className="flex items-center p-3 bg-gray-50 rounded-xl border border-gray-100">
          <div className="w-10 h-10 rounded-full bg-red-100 flex items-center justify-center text-[#EE501C] font-bold text-sm">
            {adminInfo.email ? adminInfo.email.charAt(0).toUpperCase() : 'A'}
          </div>
          <div className="ml-3 overflow-hidden">
            <p className="text-sm font-semibold text-gray-900 truncate">{adminInfo.name || 'Admin'}</p>
            <p className="text-xs text-gray-500 truncate">{adminInfo.email || 'admin@food.com'}</p>
          </div>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-4 py-4 space-y-1 overflow-y-auto">
        <div className="px-4 pb-2 text-xs font-bold text-gray-400 uppercase tracking-wider">Danh mục chính</div>
        {menuItems.map((item) => (
          <button
            key={item.id}
            onClick={() => handleMenuClick(item.path)}
            className={`w-full flex items-center px-4 py-3 text-sm font-bold rounded-xl transition-all duration-200 group ${
              isActive(item.path)
                ? 'bg-[#EE501C] text-white shadow-md shadow-orange-200'
                : 'text-gray-500 hover:bg-gray-50 hover:text-gray-900'
            }`}
          >
            <span className={`mr-3 ${isActive(item.path) ? 'text-white' : 'text-gray-400 group-hover:text-gray-600'}`}>
              {item.icon}
            </span>
            {item.label}
          </button>
        ))}
      </nav>

      {/* Footer */}
      <div className="p-4 border-t border-gray-100">
        <button 
          onClick={() => {
            onLogout();
            setMobileMenuOpen(false);
          }}
          className="w-full flex items-center px-4 py-3 text-sm font-bold text-red-600 hover:bg-red-50 rounded-xl transition-colors"
        >
          <LogOut size={20} className="mr-3" />
          Đăng xuất
        </button>
      </div>
    </>
  );

  return (
    <>
      {/* Mobile Menu Button */}
      <button
        onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
        className="md:hidden fixed top-4 left-4 z-30 p-2 bg-white rounded-lg shadow-md border border-gray-200"
      >
        {mobileMenuOpen ? <X size={24} /> : <Menu size={24} />}
      </button>

      {/* Mobile Menu Overlay */}
      {mobileMenuOpen && (
        <div
          className="md:hidden fixed inset-0 bg-black bg-opacity-50 z-30"
          onClick={() => setMobileMenuOpen(false)}
        />
      )}

      {/* Desktop Sidebar */}
      <div className="w-64 bg-white border-r border-gray-200 h-screen fixed left-0 top-0 flex flex-col z-20 hidden md:flex shadow-sm">
        <MenuContent />
      </div>

      {/* Mobile Sidebar */}
      <div
        className={`md:hidden w-64 bg-white border-r border-gray-200 h-screen fixed left-0 top-0 flex flex-col z-40 shadow-lg transform transition-transform duration-300 ${
          mobileMenuOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <MenuContent />
      </div>
    </>
  );
};
