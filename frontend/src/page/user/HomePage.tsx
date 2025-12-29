import React from 'react';

import { HeroCarousel } from '../../components/user/HeroCarousel';
import { SearchSection } from '../../components/user/SearchSection';
import { CategorySection } from '../../components/user/CategorySection';
import { PromoSection } from '../../components/user/PromoSection';

export const HomePage: React.FC = () => {
  return (
    <main className="bg-white text-gray-800 pb-12">
      {/* Hero Carousel nằm ngoài container giới hạn để tràn màn hình */}
      <HeroCarousel />
      
      {/* Các phần còn lại nằm trong container giới hạn chiều rộng */}
      <div className="max-w-[1280px] mx-auto space-y-12 mt-8">
        <SearchSection />
        <CategorySection />
        <PromoSection />
      </div>
    </main>
  );
};
