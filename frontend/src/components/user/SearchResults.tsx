
import React, { useMemo, useState } from 'react';
import { ChevronDown, Star, Heart } from 'lucide-react';
import { FoodItem, FilterOption } from '../../types/common';
import { formatNumber, normalizeString } from '../../utils';
import { FilterDropdown } from '../common/FilterDropdown';

// Filter configurations
const PRICE_OPTIONS: FilterOption[] = [
  { label: 'Dưới 50.000đ', value: 'under50' },
  { label: '50.000đ - 100.000đ', value: '50to100' },
  { label: '100.000đ - 200.000đ', value: '100to200' },
  { label: 'Trên 200.000đ', value: 'over200' },
];

const RATING_OPTIONS: FilterOption[] = [
  { label: '⭐ 4.5 trở lên', value: '4.5' },
  { label: '⭐ 4.0 trở lên', value: '4.0' },
  { label: '⭐ 3.5 trở lên', value: '3.5' },
  { label: '⭐ 3.0 trở lên', value: '3.0' },
];

const DELIVERY_TIME_OPTIONS: FilterOption[] = [
  { label: 'Dưới 15 phút', value: 'under15' },
  { label: '15 - 30 phút', value: '15to30' },
  { label: '30 - 45 phút', value: '30to45' },
  { label: 'Trên 45 phút', value: 'over45' },
];

const PROMO_OPTIONS: FilterOption[] = [
  { label: 'Giảm giá', value: 'discount' },
  { label: 'Freeship', value: 'freeship' },
  { label: 'Combo tiết kiệm', value: 'combo' },
];

interface SearchResultsProps {
  searchTerm: string;
  onItemClick: (item: FoodItem) => void;
  favoriteFoodIds: string[];
  onToggleFavorite: (id: string) => void;
  foods?: FoodItem[];
}

export const SearchResults: React.FC<SearchResultsProps> = ({
  searchTerm,
  onItemClick,
  favoriteFoodIds,
  onToggleFavorite,
  foods = []
}) => {
  // Filter states
  const [priceFilter, setPriceFilter] = useState<string | null>(null);
  const [ratingFilter, setRatingFilter] = useState<string | null>(null);
  const [deliveryTimeFilter, setDeliveryTimeFilter] = useState<string | null>(null);
  const [promoFilter, setPromoFilter] = useState<string | null>(null);

  // Pagination state
  const ITEMS_PER_PAGE = 7;
  const [displayCount, setDisplayCount] = useState(ITEMS_PER_PAGE);

  // Dropdown open states
  const [openDropdown, setOpenDropdown] = useState<string | null>(null);

  const toggleDropdown = (dropdown: string) => {
    setOpenDropdown(openDropdown === dropdown ? null : dropdown);
  };

  const clearAllFilters = () => {
    setPriceFilter(null);
    setRatingFilter(null);
    setDeliveryTimeFilter(null);
    setPromoFilter(null);
    setDisplayCount(ITEMS_PER_PAGE);
  };

  const handleShowMore = () => {
    setDisplayCount(prev => prev + ITEMS_PER_PAGE);
  };

  const hasActiveFilters = priceFilter || ratingFilter || deliveryTimeFilter || promoFilter;

  // Check if search term matches a category name
  const isCategorySearch = (term: string, categories: string[]): boolean => {
    const normalizedTerm = normalizeString(term);
    return categories.some(cat => {
      const normalizedCat = normalizeString(cat);
      return normalizedCat === normalizedTerm || normalizedCat.includes(normalizedTerm);
    });
  };

  // Helper function to parse delivery time string to minutes
  const parseDeliveryTime = (timeStr: string | undefined): number => {
    if (!timeStr) return 25;
    const numbers = timeStr.match(/\d+/g);
    if (numbers && numbers.length > 0) {
      if (numbers.length >= 2) {
        return (parseInt(numbers[0]) + parseInt(numbers[1])) / 2;
      }
      return parseInt(numbers[0]);
    }
    return 25;
  };

  // Apply additional filters (price, rating, delivery time, promo)
  const applyFilters = (items: FoodItem[]): FoodItem[] => {
    let result = [...items];

    if (priceFilter) {
      result = result.filter(item => {
        switch (priceFilter) {
          case 'under50': return item.price < 50000;
          case '50to100': return item.price >= 50000 && item.price <= 100000;
          case '100to200': return item.price >= 100000 && item.price <= 200000;
          case 'over200': return item.price > 200000;
          default: return true;
        }
      });
    }

    if (ratingFilter) {
      const minRating = parseFloat(ratingFilter);
      result = result.filter(item => item.rating >= minRating);
    }

    if (deliveryTimeFilter) {
      result = result.filter(item => {
        const deliveryMinutes = parseDeliveryTime(item.deliveryTime);
        switch (deliveryTimeFilter) {
          case 'under15': return deliveryMinutes < 15;
          case '15to30': return deliveryMinutes >= 15 && deliveryMinutes <= 30;
          case '30to45': return deliveryMinutes >= 30 && deliveryMinutes <= 45;
          case 'over45': return deliveryMinutes > 45;
          default: return true;
        }
      });
    }

    if (promoFilter) {
      result = result.filter(item => {
        if (!item.promoTag) return false;
        const normalizedPromo = normalizeString(item.promoTag);
        switch (promoFilter) {
          case 'discount': return normalizedPromo.includes('giam') || normalizedPromo.includes('sale') || normalizedPromo.includes('%');
          case 'freeship': return normalizedPromo.includes('freeship') || normalizedPromo.includes('free ship') || normalizedPromo.includes('mien phi');
          case 'combo': return normalizedPromo.includes('combo') || normalizedPromo.includes('tiet kiem');
          default: return true;
        }
      });
    }

    return result;
  };

  // Filtering logic
  const filteredFoods = useMemo(() => {
    let baseFiltered: FoodItem[] = foods;

    if (searchTerm) {
      const normalizedSearchTerm = normalizeString(searchTerm);
      const allCategories = Array.from(new Set(foods.map(f => f.category).filter(Boolean)));
      const looksLikeCategorySearch = isCategorySearch(searchTerm, allCategories);

      if (looksLikeCategorySearch) {
        const exactCategoryMatch = foods.filter(food => {
          const foodCategory = normalizeString(food.category || '');
          return foodCategory === normalizedSearchTerm;
        });

        if (exactCategoryMatch.length > 0) {
          baseFiltered = exactCategoryMatch;
        } else {
          const partialCategoryMatch = foods.filter(food => {
            const foodCategory = normalizeString(food.category || '');
            return foodCategory.includes(normalizedSearchTerm) || normalizedSearchTerm.includes(foodCategory);
          });

          if (partialCategoryMatch.length > 0) {
            baseFiltered = partialCategoryMatch;
          } else {
            baseFiltered = foods.filter(food => {
              const foodName = normalizeString(food.name || '');
              const foodDescription = normalizeString(food.description || '');
              const foodCategory = normalizeString(food.category || '');
              return foodName.includes(normalizedSearchTerm) || foodDescription.includes(normalizedSearchTerm) || foodCategory.includes(normalizedSearchTerm);
            });
          }
        }
      } else {
        baseFiltered = foods.filter(food => {
          const foodName = normalizeString(food.name || '');
          const foodDescription = normalizeString(food.description || '');
          const foodCategory = normalizeString(food.category || '');
          return foodName.includes(normalizedSearchTerm) || foodDescription.includes(normalizedSearchTerm) || foodCategory.includes(normalizedSearchTerm);
        });
      }
    }

    return applyFilters(baseFiltered);
  }, [searchTerm, foods, priceFilter, ratingFilter, deliveryTimeFilter, promoFilter]);

  const handleHeartClick = (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    onToggleFavorite(id);
  };

  return (
    <div className="max-w-4xl mx-auto px-6 py-10">
      <div className="flex items-end justify-between mb-8">
        <div>
          <p className="text-xs font-medium text-gray-400 mb-1">Kết quả tìm kiếm</p>
          <h1 className="text-3xl font-extrabold text-gray-800">"{searchTerm}"</h1>
        </div>
        <span className="text-sm text-gray-500 mb-1">
          Tìm thấy <span className="font-bold text-gray-800">{filteredFoods.length}</span> kết quả
        </span>
      </div>

      {/* Filters - sử dụng FilterDropdown component */}
      <div className="flex gap-2 mb-10 overflow-visible" style={{ overflowX: 'visible' }}>
        <button
          onClick={clearAllFilters}
          className={`${hasActiveFilters ? 'bg-white border border-gray-200 text-gray-700 hover:bg-gray-50' : 'bg-[#EE501C] text-white shadow-md shadow-orange-100'} px-5 py-2 rounded-xl text-sm font-semibold whitespace-nowrap transition-all`}
        >
          Tất cả
        </button>

        <FilterDropdown
          label="Giá"
          options={PRICE_OPTIONS}
          selectedValue={priceFilter}
          onSelect={setPriceFilter}
          isOpen={openDropdown === 'price'}
          onToggle={() => toggleDropdown('price')}
        />

        <FilterDropdown
          label="Đánh giá"
          options={RATING_OPTIONS}
          selectedValue={ratingFilter}
          onSelect={setRatingFilter}
          isOpen={openDropdown === 'rating'}
          onToggle={() => toggleDropdown('rating')}
        />

        <FilterDropdown
          label="Thời gian giao"
          options={DELIVERY_TIME_OPTIONS}
          selectedValue={deliveryTimeFilter}
          onSelect={setDeliveryTimeFilter}
          isOpen={openDropdown === 'deliveryTime'}
          onToggle={() => toggleDropdown('deliveryTime')}
        />

        <FilterDropdown
          label="Ưu đãi"
          options={PROMO_OPTIONS}
          selectedValue={promoFilter}
          onSelect={setPromoFilter}
          isOpen={openDropdown === 'promo'}
          onToggle={() => toggleDropdown('promo')}
        />
      </div>

      {/* Results List */}
      <div className="space-y-6">
        {filteredFoods.length > 0 ? (
          filteredFoods.slice(0, displayCount).map((food) => {
            const isFavorite = favoriteFoodIds.includes(food.id);
            return (
              <div
                key={food.id}
                onClick={() => onItemClick(food)}
                className="bg-white rounded-[2rem] overflow-hidden border border-gray-100 shadow-sm flex flex-col md:flex-row gap-6 p-4 md:p-6 hover:shadow-xl hover:border-orange-200 transition-all group cursor-pointer"
              >
                <div className="w-full md:w-56 h-44 shrink-0 overflow-hidden rounded-2xl relative">
                  <img src={food.imageUrl} alt={food.name} className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-500" />
                </div>
                <div className="flex-1 py-1 relative">
                  <div className="flex justify-between items-start mb-2">
                    <h3 className="text-xl font-bold text-gray-800 group-hover:text-[#EE501C] transition-colors">{food.name}</h3>
                    <button
                      onClick={(e) => handleHeartClick(e, food.id)}
                      className={`p-2 transition-colors ${isFavorite ? 'text-red-500' : 'text-gray-300 hover:text-red-300'}`}
                    >
                      <Heart className={`w-5 h-5 ${isFavorite ? 'fill-current' : ''}`} />
                    </button>
                  </div>
                  <p className="text-sm text-gray-400 mb-4 line-clamp-1">{food.description}</p>

                  <div className="flex items-center gap-4 text-xs font-semibold text-gray-500 mb-4">
                    <div className="flex items-center gap-1">
                      <Star className="w-3.5 h-3.5 text-yellow-400 fill-yellow-400" />
                      <span className="text-[#EE501C]">{typeof food.rating === 'number' ? food.rating.toFixed(1) : food.rating}</span>
                    </div>
                    <span>•</span>
                    <span>{food.distance || '1.5'} km</span>
                    <span>•</span>
                    <span>{food.deliveryTime || '15-20 phút'}</span>
                  </div>

                  {food.promoTag && (
                    <div className="mb-4">
                      <span className="text-[10px] bg-orange-50 text-[#EE501C] font-bold px-2 py-0.5 rounded border border-orange-100 uppercase tracking-wider">
                        {food.promoTag}
                      </span>
                    </div>
                  )}

                  <div className="flex items-end justify-between">
                    <div>
                      <span className="text-2xl font-black text-[#EE501C]">{formatNumber(food.price)}đ</span>
                      {food.originalPrice && <span className="text-sm text-gray-300 line-through ml-2">{formatNumber(food.originalPrice)}đ</span>}
                    </div>
                    <button className="bg-[#EE501C] text-white font-bold py-2.5 px-8 rounded-2xl shadow-lg shadow-orange-100 transform active:scale-95 transition-all">
                      Đặt ngay
                    </button>
                  </div>
                </div>
              </div>
            );
          })
        ) : (
          <div className="text-center py-20 bg-gray-50 rounded-[2rem] border border-dashed border-gray-200">
            <p className="text-gray-400 font-medium italic">Không tìm thấy món ăn phù hợp với từ khóa này.</p>
          </div>
        )}
      </div>

      {filteredFoods.length > displayCount && (
        <div className="mt-12 flex justify-center">
          <button
            onClick={handleShowMore}
            className="flex items-center gap-2 border border-gray-200 text-gray-500 font-bold py-3 px-10 rounded-2xl hover:bg-gray-50 transition-colors"
          >
            Xem thêm kết quả ({filteredFoods.length - displayCount} còn lại) <ChevronDown className="w-4 h-4" />
          </button>
        </div>
      )}
    </div>
  );
};
