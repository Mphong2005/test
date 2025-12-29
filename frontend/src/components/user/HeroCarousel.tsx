import React, { useState, useEffect } from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';

const BANNER_IMAGES = [
  "https://images.unsplash.com/photo-1504674900247-0877df9cc836?auto=format&fit=crop&q=80&w=1920&h=600",
  "https://images.unsplash.com/photo-1555939594-58d7cb561ad1?auto=format&fit=crop&q=80&w=1920&h=600",
  "https://images.unsplash.com/photo-1565299624946-b28f40a0ae38?auto=format&fit=crop&q=80&w=1920&h=600"
];

export const HeroCarousel: React.FC = () => {
  const [currentSlide, setCurrentSlide] = useState(0);

  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentSlide((prev) => (prev + 1) % BANNER_IMAGES.length);
    }, 5000);
    return () => clearInterval(timer);
  }, []);

  const nextSlide = () => setCurrentSlide((prev) => (prev + 1) % BANNER_IMAGES.length);
  const prevSlide = () => setCurrentSlide((prev) => (prev - 1 + BANNER_IMAGES.length) % BANNER_IMAGES.length);

  return (
    // Sử dụng p-2 hoặc p-4 để tạo khoảng hở nhỏ giúp bo tròn hiển thị rõ ràng và đẹp mắt
    <section className="w-full p-3 md:p-6">
      <div className="relative group w-full h-[300px] md:h-[500px] rounded-[2.5rem] overflow-hidden shadow-2xl shadow-orange-100/40">
        <div 
          className="flex transition-transform duration-700 ease-in-out h-full"
          style={{ transform: `translateX(-${currentSlide * 100}%)` }}
        >
          {BANNER_IMAGES.map((img, idx) => (
            <div key={idx} className="min-w-full h-full relative">
              <img 
                src={img} 
                alt={`Banner ${idx + 1}`} 
                className="w-full h-full object-cover"
              />
              <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-black/20 to-transparent flex flex-col justify-end p-8 md:pb-16 md:px-20 text-white">
                <div className="max-w-[1280px] mx-auto w-full">
                  <h1 className="text-3xl md:text-6xl font-black mb-4 leading-tight drop-shadow-lg">Món ngon <br/> giao tận cửa</h1>
                  <p className="text-sm md:text-xl opacity-90 max-w-lg hidden sm:block font-medium drop-shadow-md">Khám phá hàng ngàn món ăn hấp dẫn từ các nhà hàng uy tín nhất.</p>
                </div>
              </div>
            </div>
          ))}
        </div>

        <button 
          onClick={prevSlide}
          className="absolute left-4 md:left-10 top-1/2 -translate-y-1/2 w-12 h-12 rounded-full bg-white/20 backdrop-blur-md text-white flex items-center justify-center opacity-0 group-hover:opacity-100 transition-all hover:bg-white/30 border border-white/30"
        >
          <ChevronLeft className="w-8 h-8" />
        </button>
        <button 
          onClick={nextSlide}
          className="absolute right-4 md:right-10 top-1/2 -translate-y-1/2 w-12 h-12 rounded-full bg-white/20 backdrop-blur-md text-white flex items-center justify-center opacity-0 group-hover:opacity-100 transition-all hover:bg-white/30 border border-white/30"
        >
          <ChevronRight className="w-8 h-8" />
        </button>

        <div className="absolute bottom-6 left-1/2 -translate-x-1/2 flex gap-3">
          {BANNER_IMAGES.map((_, idx) => (
            <button
              key={idx}
              onClick={() => setCurrentSlide(idx)}
              className={`h-2 rounded-full transition-all duration-300 shadow-sm ${
                currentSlide === idx ? 'w-10 bg-[#EE501C]' : 'w-2.5 bg-white/60 hover:bg-white'
              }`}
            />
          ))}
        </div>
      </div>
    </section>
  );
};