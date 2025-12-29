
import React, { useState, useEffect, useRef } from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { getCategoriesApi } from '../../api/productApi';
import { useAuthContext } from '../../contexts/AuthContext';
import { LoginRequestModal } from '../common/LoginRequestModal';

export const CategorySection: React.FC = () => {
  const navigate = useNavigate();
  const { isAuthenticated } = useAuthContext();
  const [categories, setCategories] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [showLoginModal, setShowLoginModal] = useState(false);
  const scrollContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const fetchCategories = async () => {
      try {
        const data = await getCategoriesApi();
        setCategories(data);
      } catch (error) {
        console.error('Failed to fetch categories', error);
      } finally {
        setLoading(false);
      }
    };
    fetchCategories();
  }, []);

  const handleCategoryClick = (categoryName: string) => {
    if (!isAuthenticated) {
      setShowLoginModal(true);
      return;
    }
    navigate('/search', { state: { query: categoryName } });
  };

  const scrollLeft = () => {
    if (scrollContainerRef.current) {
      scrollContainerRef.current.scrollBy({ left: -300, behavior: 'smooth' });
    }
  };

  const scrollRight = () => {
    if (scrollContainerRef.current) {
      scrollContainerRef.current.scrollBy({ left: 300, behavior: 'smooth' });
    }
  };

  if (loading) return <div className="h-40 flex items-center justify-center text-gray-400">Đang tải danh mục...</div>;

  return (
    <section className="px-4 md:px-10">
      <div className="flex items-center justify-between mb-8">
        <h2 className="text-xl font-bold text-gray-800">There's something for everyone!</h2>
        <div className="flex gap-2">
          <button 
            onClick={scrollLeft}
            className="p-2 rounded-full border border-gray-200 hover:bg-gray-50 hover:border-[#EE501C] transition-colors hidden md:flex"
          >
            <ChevronLeft className="w-5 h-5 text-gray-400 hover:text-[#EE501C]" />
          </button>
          <button 
            onClick={scrollRight}
            className="p-2 rounded-full border border-gray-200 hover:bg-gray-50 hover:border-[#EE501C] transition-colors hidden md:flex"
          >
            <ChevronRight className="w-5 h-5 text-gray-400 hover:text-[#EE501C]" />
          </button>
        </div>
      </div>
      
      <div 
        ref={scrollContainerRef}
        className="flex overflow-x-auto gap-4 md:gap-6 pb-4 -mx-2 px-2"
        style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
      >
        <style>{`
          div::-webkit-scrollbar {
            display: none;
          }
        `}</style>
        {categories.map((cat) => (
          <div 
            key={cat.id} 
            onClick={() => handleCategoryClick(cat.name)}
            className="flex flex-col items-center gap-2 cursor-pointer group min-w-[90px] md:min-w-[110px] lg:min-w-[140px] flex-shrink-0"
          >
            <div className="w-20 h-20 md:w-28 md:h-28 lg:w-32 lg:h-32 rounded-[2rem] lg:rounded-[2.5rem] bg-gray-50 overflow-hidden shadow-sm border border-gray-100 group-hover:shadow-2xl group-hover:scale-105 group-hover:border-[#EE501C] transition-all duration-300">
              <img src={cat.image} alt={cat.name} className="w-full h-full object-cover grayscale-[15%] group-hover:grayscale-0 transition-all duration-500" />
            </div>
            <span className="text-[10px] md:text-xs font-black text-gray-700 group-hover:text-[#EE501C] transition-colors text-center uppercase tracking-widest">{cat.name}</span>
          </div>
        ))}
      </div>

      <LoginRequestModal 
        isOpen={showLoginModal} 
        onClose={() => setShowLoginModal(false)} 
      />
    </section>
  );
};
