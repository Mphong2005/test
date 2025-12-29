import React, { useState, useEffect } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { Globe, Home, ClipboardList, User, ChevronLeft, Search } from 'lucide-react';
import { useAuthContext } from '../../contexts/AuthContext';

export const Header: React.FC = () => {
  const { isAuthenticated } = useAuthContext();
  const location = useLocation();
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState('');

  // Helper to check active route for styling
  const isActive = (path: string) => location.pathname === path;
  const isHomePage = location.pathname === '/';
  const isSearchPage = location.pathname === '/search';
  
  // Check if we are in "Results" mode (have a query)
  const hasSearchQuery = isSearchPage && location.state?.query;

  // Logic to show/hide search bar
  // Only show on Search Results page (when we have a query on the search page)
  const showSearchBar = !!hasSearchQuery;

  // Sync search query from location state to display in header (read-only view)
  useEffect(() => {
    if (hasSearchQuery) {
      setSearchQuery(location.state.query);
    } else {
      setSearchQuery('');
    }
  }, [location.pathname, location.state, hasSearchQuery]);

  const handleSearchClick = () => {
    // Navigate to search overlay (reset query)
    navigate('/search');
  };

  return (
    <header className="fixed top-0 left-0 z-50 flex h-[80px] w-full items-center justify-center bg-white px-4 shadow-sm md:px-10 transition-all">
      <div className="flex w-full max-w-[1280px] items-center justify-between gap-4">
        
        <div className="flex items-center gap-4 shrink-0">
          {/* Back Button - Only show if not on Home Page */}
          {!isHomePage && (
            <button 
              onClick={() => navigate(-1)}
              className="p-2 -ml-2 text-gray-600 hover:text-[#EE501C] hover:bg-gray-50 rounded-full transition-colors"
            >
              <ChevronLeft size={24} />
            </button>
          )}

          <Link to="/" className="flex items-center gap-2">
            {/* Logo inline SVG */}
            <svg 
              xmlns="http://www.w3.org/2000/svg" 
              viewBox="0 0 48 48" 
              fill="none"
              className="h-10 w-10"
            >
              <circle cx="24" cy="24" r="24" fill="#EE501C"/>
              <g stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" fill="none" transform="translate(12, 10)">
                <path d="M3 2v7a4 4 0 0 0 4 4v13"/>
                <path d="M3 2h4"/>
                <path d="M5 2v7"/>
                <path d="M21 15V2a5 5 0 0 0-5 5v6c0 1.1.9 2 2 2h3v13"/>
              </g>
            </svg>
            <span className="text-xl font-bold tracking-tight text-[#EE501C] md:text-2xl hidden sm:block">
              FoodDelivery
            </span>
          </Link>
        </div>

        {/* Header Search Bar */}
        {showSearchBar && (
          <div className="hidden md:flex flex-1 max-w-lg mx-4">
            <div 
              onClick={handleSearchClick}
              className="w-full relative group cursor-pointer"
            >
              <input 
                type="text" 
                readOnly
                value={searchQuery}
                placeholder="Tìm kiếm món ăn, quán ăn..." 
                className="w-full py-2.5 pl-10 pr-10 rounded-full border border-gray-200 bg-gray-50 hover:bg-white hover:border-[#EE501C] hover:ring-2 hover:ring-orange-50 outline-none transition-all text-sm text-gray-700 placeholder-gray-400 cursor-pointer"
              />
              <Search className="absolute left-3.5 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4 transition-colors" />
            </div>
          </div>
        )}

        {isAuthenticated ? (
          /* Logged In View */
          <div className="flex items-center gap-6 md:gap-8 shrink-0">
            {/* TRANG CHỦ */}
            <Link 
              to="/" 
              className={`flex flex-col items-center gap-1 transition-colors ${
                isActive('/') ? 'text-[#EE501C]' : 'text-gray-500 hover:text-[#EE501C]'
              }`}
            >
              <Home size={22} strokeWidth={isActive('/') ? 2.5 : 2} />
              <span className="text-[10px] font-bold uppercase tracking-wide hidden sm:block">Trang chủ</span>
            </Link>

            {/* ĐƠN HÀNG */}
            <Link 
              to="/orders" 
              className={`flex flex-col items-center gap-1 transition-colors ${
                isActive('/orders') ? 'text-[#EE501C]' : 'text-gray-500 hover:text-[#EE501C]'
              }`}
            >
              <ClipboardList size={22} strokeWidth={isActive('/orders') ? 2.5 : 2} />
              <span className="text-[10px] font-bold uppercase tracking-wide hidden sm:block">Đơn hàng</span>
            </Link>

            {/* TÔI (Direct Link to Profile) */}
            <Link 
              to="/profile"
              className={`flex flex-col items-center gap-1 transition-colors ${
                isActive('/profile') ? 'text-[#EE501C]' : 'text-gray-500 hover:text-[#EE501C]'
              }`}
            >
              <User size={22} strokeWidth={isActive('/profile') ? 2.5 : 2} />
              <span className="text-[10px] font-bold uppercase tracking-wide hidden sm:block">Tôi</span>
            </Link>
          </div>
        ) : (
          /* Guest View */
          <div className="flex items-center gap-3 md:gap-4 shrink-0">
            <Link
              to="/login"
              className="hidden rounded-full border border-gray-300 px-5 py-2 text-sm font-bold text-gray-700 transition-colors hover:bg-gray-50 hover:border-gray-400 sm:block"
            >
              Login
            </Link>
            <Link
              to="/register"
              className="rounded-full border border-[#EE501C] bg-[#EE501C] px-5 py-2 text-sm font-bold text-white shadow-sm transition-transform hover:bg-[#d43f0f] active:scale-95"
            >
              Sign up
            </Link>
            <button
              type="button"
              className="flex items-center gap-1 text-sm font-bold text-gray-500 transition-colors hover:text-[#EE501C] bg-gray-50 px-3 py-2 rounded-full"
            >
              <Globe size={16} />
              <span>VIE</span>
            </button>
          </div>
        )}
      </div>
    </header>
  );
};