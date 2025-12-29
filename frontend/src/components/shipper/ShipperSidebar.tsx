
import React from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { ShipperProfile } from '../../types/shipper';
import { Home, History, ClipboardList, User, LogOut } from 'lucide-react';

interface SidebarProps {
  onLogout: () => void;
  profile: ShipperProfile;
}

export const ShipperSidebar: React.FC<SidebarProps> = ({ onLogout, profile }) => {
  const location = useLocation();
  const navigate = useNavigate();
  const currentPath = location.pathname;

  const navItems = [
    { path: '/shipper/home', label: 'Trang chủ', icon: Home },
    { path: '/shipper/history', label: 'Lịch sử', icon: History },
    { path: '/shipper/pending', label: 'Đơn hàng cần xử lý', icon: ClipboardList },
    { path: '/shipper/profile', label: 'Tôi', icon: User },
  ];

  const isActive = (path: string) => currentPath === path;

  return (
    <aside className="w-20 lg:w-72 bg-white border-r border-[#e7d5cf] flex flex-col justify-between shrink-0 transition-all duration-300 z-30 h-screen fixed left-0 top-0 shadow-lg shadow-orange-50">
      <div className="flex flex-col h-full p-6 gap-8">
        {/* User Info - Only show if data exists */}
        {profile.email ? (
          <div className="flex items-center gap-4 pb-6 border-b border-[#f3eae7]">
            <div 
              className="shrink-0 w-14 h-14 rounded-full bg-gradient-to-br from-[#EE501C] to-[#FF7043] border-2 border-[#EE501C] shadow-md flex items-center justify-center text-white text-xl font-black"
            >
              {profile.name ? profile.name.charAt(0).toUpperCase() : 'S'}
            </div>
            <div className="hidden lg:flex flex-col overflow-hidden">
              <h1 className="text-gray-900 text-lg font-bold leading-tight truncate">{profile.name}</h1>
              <p className="text-gray-400 text-xs font-medium truncate">{profile.email}</p>
            </div>
          </div>
        ) : (
          <div className="h-[88px] border-b border-[#f3eae7] flex items-center justify-center">
             <div className="animate-pulse w-14 h-14 bg-gray-200 rounded-full"></div>
          </div>
        )}
        
        {/* Navigation */}
        <nav className="flex flex-col gap-3 flex-1">
          {navItems.map((item) => (
            <button
              key={item.path}
              onClick={() => navigate(item.path)}
              className={`flex items-center gap-4 px-5 py-4 rounded-[1.8rem] transition-all group w-full text-left relative overflow-hidden ${
                isActive(item.path)
                  ? 'bg-[#EE501C]/10 text-[#EE501C]' 
                  : 'hover:bg-gray-50 text-gray-500'
              }`}
            >
              <item.icon 
                size={24} 
                strokeWidth={isActive(item.path) ? 2.5 : 2}
                className={`transition-colors ${isActive(item.path) ? 'text-[#EE501C]' : 'text-gray-400 group-hover:text-gray-600'}`} 
              />
              <p className={`hidden lg:block text-sm font-bold ${isActive(item.path) ? 'text-[#EE501C]' : 'text-gray-600'}`}>
                {item.label}
              </p>
              {isActive(item.path) && (
                <div className="absolute right-0 top-1/2 -translate-y-1/2 w-1.5 h-8 bg-[#EE501C] rounded-l-full hidden lg:block" />
              )}
            </button>
          ))}
        </nav>

        {/* Logout Button - Pill Shape, Text Left, Icon Right */}
        <div className="pt-4 border-t border-[#f3eae7]">
            <button 
            onClick={onLogout}
            className="flex w-full cursor-pointer items-center justify-center gap-2 overflow-hidden rounded-full py-4 px-6 border border-[#e7d5cf] hover:bg-gray-50 text-gray-900 font-bold shadow-sm transition-all active:scale-95 group bg-white"
            >
            <span className="truncate hidden lg:block text-sm">Đăng xuất</span>
            <LogOut size={20} className="text-gray-900 group-hover:text-[#EE501C] transition-colors" />
            </button>
        </div>
      </div>
    </aside>
  );
};
