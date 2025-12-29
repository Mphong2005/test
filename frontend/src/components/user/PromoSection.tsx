
import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getPromotionsApi } from '../../api/productApi';
import { useAuthContext } from '../../contexts/AuthContext';
import { LoginRequestModal } from '../common/LoginRequestModal';

export const PromoSection: React.FC = () => {
  const navigate = useNavigate();
  const { isAuthenticated } = useAuthContext();
  const [promotions, setPromotions] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [showLoginModal, setShowLoginModal] = useState(false);

  useEffect(() => {
    const fetchPromos = async () => {
      try {
        const data = await getPromotionsApi();
        console.log('Promotions loaded:', data);
        if (data && Array.isArray(data) && data.length > 0) {
          setPromotions(data);
        } else {
          console.warn('No promotions data received');
        }
      } catch (error) {
        console.error('Error fetching promotions:', error);
      } finally {
        setLoading(false);
      }
    };
    fetchPromos();
  }, []);

  const handlePromoClick = (foodId: string) => {
    if (!isAuthenticated) {
      setShowLoginModal(true);
      return;
    }
    navigate(`/product/${foodId}`);
  };

  if (loading) return <div className="h-60 flex items-center justify-center text-gray-400">Đang tải khuyến mãi...</div>;

  if (!promotions || promotions.length === 0) {
    return (
      <section className="px-4 md:px-10 pb-12">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-bold text-gray-800">
            FoodDelivery Promotion in <span className="text-[#EE501C]">Hanoi</span>
          </h2>
        </div>
        <div className="text-center py-12 text-gray-400">
          Không có khuyến mãi nào tại thời điểm này.
        </div>
      </section>
    );
  }

  return (
    <section className="px-4 md:px-10 pb-12">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-bold text-gray-800">
          FoodDelivery Promotion in <span className="text-[#EE501C]">Hanoi</span>
        </h2>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
        {promotions.slice(0, 8).map((promo) => (
          <div
            key={promo.id}
            onClick={() => handlePromoClick(promo.foodId)}
            className="bg-white rounded-3xl overflow-hidden shadow-sm border border-gray-100 hover:shadow-xl hover:-translate-y-1 transition-all cursor-pointer group"
          >
            <div className="relative h-44 overflow-hidden">
              <img src={promo.image} alt={promo.name} className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-500" />
              <span className="absolute top-3 left-3 bg-yellow-400 text-[10px] font-bold px-2 py-1 rounded flex items-center gap-1 shadow-sm">
                <span className="text-xs">🏷️</span> PROMO
              </span>
            </div>
            <div className="p-4">
              <h3 className="font-bold text-gray-800 truncate mb-1">{promo.name}</h3>
              <div className="flex items-center justify-between">
                <span className="text-[10px] text-gray-400 uppercase tracking-tight font-bold">{promo.vendor}</span>
                <span className="text-xs font-bold text-[#EE501C] group-hover:underline">{promo.action}</span>
              </div>
            </div>
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
