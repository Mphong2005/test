
import React, { useState, useEffect } from 'react';
import { useNavigate, useParams, useLocation } from 'react-router-dom';
import { Star, MapPin, Clock, Plus, Minus, ShoppingCart, ChevronRight, ChevronLeft, Heart, Lock } from 'lucide-react';
import { FoodItem, Voucher, Review, Restaurant } from '../../types/common';
import { getFoodByIdApi, getFoodsApi } from '../../api/productApi';
import { getAvailableVouchersForUserApi } from '../../api/voucherApi';
import { getReviewsByFoodIdApi } from '../../api/reviewApi';
import { getRestaurantByIdApi } from '../../api/restaurantApi';
import { addToCartApi } from '../../api/cartApi';
import { useAuthContext } from '../../contexts/AuthContext';
import { LoginRequestModal } from '../../components/common/LoginRequestModal';
import { formatNumber, getInitials } from '../../utils';

export const ProductDetailPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { id } = useParams();
  const { isAuthenticated } = useAuthContext();

  const [quantity, setQuantity] = useState(1);
  const [food, setFood] = useState<FoodItem | null>(null);
  const [restaurant, setRestaurant] = useState<Restaurant | null>(null);
  const [vouchers, setVouchers] = useState<Voucher[]>([]);
  const [relatedFoods, setRelatedFoods] = useState<FoodItem[]>([]);
  const [reviews, setReviews] = useState<Review[]>([]);
  const [loading, setLoading] = useState(true);
  const [restaurantRating, setRestaurantRating] = useState<number>(0);
  const [restaurantReviewCount, setRestaurantReviewCount] = useState<number>(0);
  
  // Cart items state - CẦN DÙNG STATE ĐỂ CẬP NHẬT KHI QUAY LẠI TỪ CHECKOUT
  const locationState = location.state as any;
  const [cartItems, setCartItems] = useState<Array<{ food: FoodItem, quantity: number }>>(
    locationState?.currentCartItems || []
  );
  
  // Cập nhật cartItems khi location.state thay đổi (quay lại từ checkout)
  useEffect(() => {
    if (locationState?.currentCartItems) {
      setCartItems(locationState.currentCartItems);
    }
  }, [JSON.stringify(locationState?.currentCartItems)]); // stringify để so sánh deep
  
  const cartItemCount = cartItems.reduce((sum: number, item: any) => sum + item.quantity, 0);
  const cartTotal = cartItems.reduce((sum: number, item: any) => sum + (item.food.price * item.quantity), 0);
  
  // Carousel state for related foods
  const [currentPage, setCurrentPage] = useState(0);
  const itemsPerPage = 4;
  const totalPages = Math.ceil(relatedFoods.length / itemsPerPage);
  const displayedFoods = relatedFoods.slice(currentPage * itemsPerPage, (currentPage + 1) * itemsPerPage);

  // Login Modal State
  const [showLoginModal, setShowLoginModal] = useState(false);

  // Reset quantity to 1 when food ID changes
  useEffect(() => {
    setQuantity(1); // Always reset to 1 when viewing a new food item
  }, [id]); // Reset whenever food ID changes

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        if (id) {
          // 1. Fetch current food
          const foodData = await getFoodByIdApi(id);
          setFood(foodData || null);

          // 2. Fetch vouchers (only if authenticated, otherwise empty array)
          if (isAuthenticated && foodData?.restaurantId) {
            try {
              const voucherData = await getAvailableVouchersForUserApi(foodData.restaurantId);
              setVouchers(voucherData);
            } catch (error) {
              console.error('Error fetching vouchers:', error);
              setVouchers([]);
            }
          } else {
            setVouchers([]);
          }

          // 3. Fetch Reviews for this food
          const reviewsData = await getReviewsByFoodIdApi(id);
          setReviews(reviewsData);
          
          // Store current food rating for restaurant calculation
          const currentFoodReviewCount = reviewsData.length;
          const currentFoodRating = currentFoodReviewCount > 0
            ? Number((reviewsData.reduce((acc, r) => acc + r.rating, 0) / currentFoodReviewCount).toFixed(1))
            : 0;

          // 4. Fetch Restaurant & Related Foods
          if (foodData && foodData.restaurantId) {
            console.log('Fetching restaurant with ID:', foodData.restaurantId);
            const resData = await getRestaurantByIdApi(foodData.restaurantId);
            console.log('Restaurant data received:', resData);
            setRestaurant(resData || null);

            // Get related foods from restaurant menu (more efficient than fetching all foods)
            if (resData && resData.menu && Array.isArray(resData.menu)) {
              console.log('Menu found, length:', resData.menu.length);
              const related: FoodItem[] = [];
              const currentFoodId = id || '';
              console.log('Current food ID:', currentFoodId);

              // Iterate through menu categories and items
              for (const category of resData.menu) {
                console.log('Processing category:', category.category, 'items:', category.items?.length || 0);
                if (category.items && Array.isArray(category.items)) {
                  for (const item of category.items) {
                    // Create food ID in the same format as backend: restaurantId-foodName
                    const foodId = `${foodData.restaurantId}-${item.name}`;
                    console.log('Checking item:', item.name, 'foodId:', foodId, 'status:', item.status);

                    // Skip current food item and inactive items
                    if (foodId !== currentFoodId && item.status !== false && item.status !== 'Inactive') {
                      related.push({
                        id: foodId,
                        name: item.name || '',
                        price: item.price || 0,
                        description: item.description || '',
                        imageUrl: item.image || item.imageUrl || '',
                        category: category.category || '',
                        restaurantId: foodData.restaurantId,
                        rating: resData.rating || 4.0,
                        deliveryTime: '15-20 phút',
                      });
                      console.log('Added related food:', item.name);

                      // No limit - get all foods from the restaurant
                    }
                  }
                }
              }

              console.log('Total related foods found:', related.length);
              
              // Fetch reviews for all related foods to get actual ratings
              const relatedWithRatings = await Promise.all(
                related.map(async (food) => {
                  try {
                    const foodReviews = await getReviewsByFoodIdApi(food.id);
                    const reviewCount = foodReviews.length;
                    const averageRating = reviewCount > 0
                      ? Number((foodReviews.reduce((acc, r) => acc + r.rating, 0) / reviewCount).toFixed(1))
                      : 0;
                    return {
                      ...food,
                      rating: averageRating,
                      reviewCount: reviewCount,
                    };
                  } catch (error) {
                    console.error(`Error fetching reviews for ${food.id}:`, error);
                    return {
                      ...food,
                      rating: 0,
                      reviewCount: 0,
                    };
                  }
                })
              );
              
              setRelatedFoods(relatedWithRatings);
              
              // Calculate restaurant rating from ALL foods (current food + related foods)
              // Use already fetched current food reviews
              // Calculate average rating of all foods (simple average, not weighted)
              const allFoodsWithRatings = [
                { rating: currentFoodRating, reviewCount: currentFoodReviewCount },
                ...relatedWithRatings.map(f => ({ rating: f.rating, reviewCount: f.reviewCount || 0 }))
              ];
              
              // Filter out foods with no reviews (rating = 0 and reviewCount = 0)
              const foodsWithReviews = allFoodsWithRatings.filter(f => f.reviewCount > 0);
              
              const totalReviewCount = allFoodsWithRatings.reduce((sum, f) => sum + f.reviewCount, 0);
              
              // Simple average of food ratings (not weighted by review count)
              const restaurantAvgRating = foodsWithReviews.length > 0
                ? Number((foodsWithReviews.reduce((sum, f) => sum + f.rating, 0) / foodsWithReviews.length).toFixed(1))
                : 0;
              
              setRestaurantRating(restaurantAvgRating);
              setRestaurantReviewCount(totalReviewCount);
            } else {
              console.log('Menu not available, using fallback API');
              // Fallback: Fetch related foods from API if menu is not available
              const allFoods = await getFoodsApi();
              const related = allFoods
                .filter(f => f.restaurantId === foodData.restaurantId && f.id !== id);
              console.log('Fallback related foods:', related.length);
              
              // Fetch reviews for all related foods to get actual ratings
              const relatedWithRatings = await Promise.all(
                related.map(async (food) => {
                  try {
                    const foodReviews = await getReviewsByFoodIdApi(food.id);
                    const reviewCount = foodReviews.length;
                    const averageRating = reviewCount > 0
                      ? Number((foodReviews.reduce((acc, r) => acc + r.rating, 0) / reviewCount).toFixed(1))
                      : 0;
                    return {
                      ...food,
                      rating: averageRating,
                      reviewCount: reviewCount,
                    };
                  } catch (error) {
                    console.error(`Error fetching reviews for ${food.id}:`, error);
                    return {
                      ...food,
                      rating: food.rating || 0,
                      reviewCount: 0,
                    };
                  }
                })
              );
              
              setRelatedFoods(relatedWithRatings);
              
              // Calculate restaurant rating from ALL foods (current food + related foods)
              // Use already fetched current food reviews
              // Calculate average rating of all foods (simple average, not weighted)
              const allFoodsWithRatings = [
                { rating: currentFoodRating, reviewCount: currentFoodReviewCount },
                ...relatedWithRatings.map(f => ({ rating: f.rating, reviewCount: f.reviewCount || 0 }))
              ];
              
              // Filter out foods with no reviews (rating = 0 and reviewCount = 0)
              const foodsWithReviews = allFoodsWithRatings.filter(f => f.reviewCount > 0);
              
              const totalReviewCount = allFoodsWithRatings.reduce((sum, f) => sum + f.reviewCount, 0);
              
              // Simple average of food ratings (not weighted by review count)
              const restaurantAvgRating = foodsWithReviews.length > 0
                ? Number((foodsWithReviews.reduce((sum, f) => sum + f.rating, 0) / foodsWithReviews.length).toFixed(1))
                : 0;
              
              setRestaurantRating(restaurantAvgRating);
              setRestaurantReviewCount(totalReviewCount);
            }
          }
        }
      } catch (error) {
        console.error("Error fetching product detail", error);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [id]);

  // Auto scroll to hash (e.g. #related-foods)
  useEffect(() => {
    if (location.hash && !loading) {
      const element = document.getElementById(location.hash.replace('#', ''));
      if (element) {
        setTimeout(() => {
          element.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 100);
      }
    }
  }, [location.hash, loading]);

  if (loading) {
    return <div className="min-h-screen flex items-center justify-center">Đang tải...</div>;
  }

  if (!food) {
    return <div className="min-h-screen flex items-center justify-center">Không tìm thấy món ăn</div>;
  }

  // Calculate dynamic stats from ACTUAL reviews to ensure sync
  // Đồng bộ dữ liệu đánh giá: Tính toán trực tiếp từ mảng reviews
  const totalReviews = reviews.length;
  const averageRating = totalReviews > 0
    ? (reviews.reduce((acc, r) => acc + r.rating, 0) / totalReviews).toFixed(1)
    : "0.0"; // Default or fallback if no reviews

  // Use fetched restaurant or fallback to a default structure if missing
  const displayRestaurant = restaurant ? {
    ...restaurant,
    initial: restaurant.initial || getInitials(restaurant.name || 'R'),
    address: restaurant.address || 'Đang cập nhật địa chỉ',
  } : {
    id: 'unknown',
    name: 'Nhà hàng đối tác',
    address: 'Đang cập nhật địa chỉ',
    imageUrl: '',
    rating: 4.5,
    reviewsCount: 0,
    initial: 'R',
    status: 'Active'
  };

  const totalPrice = food.price * quantity;

  const handleOrderNow = async () => {
    if (!isAuthenticated) {
      setShowLoginModal(true);
      return;
    }

    // REMOVED: Logic tự động thêm vào cart (yêu thích) khi đặt món
    // User chỉ muốn đặt món, không muốn tự động thêm vào yêu thích
    // Nếu muốn thêm vào yêu thích, user phải click nút "Yêu thích" riêng

    // Check if coming from checkout page
    const locationState = location.state as any;
    if (locationState?.fromCheckout && locationState?.currentCartItems) {
      // Add current food to existing cart
      const existingItems = locationState.currentCartItems || [];
      const existingIndex = existingItems.findIndex((item: any) => item.food.id === food.id);

      let updatedItems;
      if (existingIndex >= 0) {
        // Item already exists: CỘNG thêm số lượng
        updatedItems = [...existingItems];
        updatedItems[existingIndex].quantity += quantity; // CỘNG thêm
        // Move to front to show it first
        const item = updatedItems.splice(existingIndex, 1)[0];
        updatedItems.unshift(item);
      } else {
        // New item: add with current quantity from state
        // Add new item to the beginning so it shows first
        updatedItems = [{ food, quantity: quantity }, ...existingItems];
      }

      // Navigate back to checkout with updated items
      navigate('/checkout', { state: { items: updatedItems } });
    } else {
      // Normal flow: create new order with current quantity (user can choose quantity)
      navigate('/checkout', { state: { food, quantity: quantity || 1 } });
    }
  };

  const handleApplyVoucher = (voucher: Voucher) => {
    if (!isAuthenticated) {
      setShowLoginModal(true);
      return;
    }
    navigate('/checkout', { state: { food, quantity, voucher } });
  };

  const handleViewReviews = () => {
    navigate(`/product/${food.id}/reviews`);
  };

  const handleViewAllVouchers = () => {
    if (!isAuthenticated) {
      setShowLoginModal(true);
      return;
    }
    // Navigate to profile page and switch to VOUCHERS tab
    navigate('/profile', { state: { view: 'VOUCHERS' } });
  };

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-8 py-10 space-y-12 animate-in fade-in duration-500 bg-white pb-24">
      {/* Breadcrumb */}
      <nav className="text-xs font-medium text-gray-400 flex items-center gap-2 select-none">
        <button
          onClick={() => navigate('/')}
          className="hover:text-[#EE501C] transition-colors cursor-pointer"
        >
          Trang chủ
        </button>
        <ChevronRight className="w-3 h-3 shrink-0" />
        <button
          className="hover:text-[#EE501C] transition-colors cursor-pointer"
        >
          Món ăn
        </button>
        <ChevronRight className="w-3 h-3 shrink-0" />
        <span className="text-gray-800 font-semibold truncate max-w-[150px] md:max-w-none">{food.name}</span>
      </nav>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-12">
        {/* Left: Image */}
        <div className="space-y-4">
          <div className="relative rounded-[2.5rem] overflow-hidden shadow-2xl h-[300px] md:h-[500px]">
            <img src={food.imageUrl} alt={food.name} className="w-full h-full object-cover" />
          </div>
        </div>

        {/* Right: Info */}
        <div className="py-2 space-y-8">
          <div>
            <div className="flex items-center justify-between mb-2 gap-4">
              <h1 className="text-3xl md:text-4xl font-black text-gray-900 leading-tight">{food.name}</h1>
              <span className="text-[10px] md:text-xs bg-green-50 text-green-600 font-bold px-3 py-1 rounded-full border border-green-100 flex items-center gap-1 shrink-0">
                <span className="w-1.5 h-1.5 rounded-full bg-green-500"></span> Còn hàng
              </span>
            </div>
            <div className="flex items-baseline gap-3">
              <div className="text-3xl font-black text-[#EE501C]">{formatNumber(food.price)}đ</div>
              {food.originalPrice && (
                <span className="text-lg text-gray-300 line-through font-bold">{formatNumber(food.originalPrice)}đ</span>
              )}
            </div>
          </div>

          {/* Restaurant Info */}
          <div className="bg-gray-50 p-6 rounded-3xl border border-gray-100 flex items-center gap-5">
            <div className="w-14 h-14 bg-white rounded-full flex items-center justify-center text-[#EE501C] font-bold text-xl shadow-sm overflow-hidden shrink-0 border-2 border-orange-50">
              {displayRestaurant.initial || displayRestaurant.name.charAt(0)}
            </div>
            <div className="flex-1">
              <div className="flex items-center justify-between">
                <h3 className="font-bold text-gray-800">{displayRestaurant.name}</h3>
                {displayRestaurant.status === 'Active' ? (
                  <span className="text-[10px] text-green-500 font-bold border border-green-200 bg-white px-2 py-0.5 rounded">Đang mở cửa</span>
                ) : (
                  <span className="text-[10px] text-red-500 font-bold border border-red-200 bg-white px-2 py-0.5 rounded">Đóng cửa</span>
                )}
              </div>
              <div className="flex items-center gap-1 text-[11px] text-gray-400 mb-1 mt-1">
                <MapPin className="w-3 h-3" /> {displayRestaurant.address}
              </div>
              {/* Updated Rating Section: Clickable and uses restaurant rating (average of all foods) */}
              <div
                onClick={handleViewReviews}
                className="flex items-center gap-1 text-[11px] text-[#EE501C] font-bold cursor-pointer hover:underline"
              >
                <Star className="w-3 h-3 fill-[#EE501C]" /> {restaurantRating > 0 ? restaurantRating.toFixed(1) : '0.0'} <span className="text-gray-400 font-medium">({restaurantReviewCount} đánh giá)</span>
              </div>
            </div>
          </div>

          {/* Description */}
          <div>
            <h4 className="font-bold text-gray-800 mb-2">Mô tả</h4>
            <p className="text-sm text-gray-500 leading-relaxed italic">{food.description}</p>
          </div>

          {/* Quantity & Time */}
          <div className="flex flex-col sm:flex-row items-start sm:items-center gap-6 sm:gap-8 border-y border-gray-100 py-6">
            <div className="flex items-center gap-4 bg-gray-50 p-2 rounded-2xl border border-gray-100 w-full sm:w-auto justify-between sm:justify-start">
              <button
                onClick={() => setQuantity(q => Math.max(1, q - 1))}
                className="w-10 h-10 bg-white rounded-xl shadow-sm flex items-center justify-center text-gray-500 hover:text-[#EE501C] transition-colors"
              >
                <Minus className="w-4 h-4" />
              </button>
              <span className="w-8 text-center font-bold text-gray-800">{quantity}</span>
              <button
                onClick={() => setQuantity(q => q + 1)}
                className="w-10 h-10 bg-white rounded-xl shadow-sm flex items-center justify-center text-gray-500 hover:text-[#EE501C] transition-colors"
              >
                <Plus className="w-4 h-4" />
              </button>
            </div>
            <div className="flex flex-col gap-0.5">
              <div className="flex items-center gap-2 text-[10px] text-orange-700 font-bold bg-orange-50 w-fit px-2 py-0.5 rounded-full">
                <Clock className="w-3 h-3" /> THỜI GIAN GIAO HÀNG
              </div>
              <span className="text-sm font-bold text-gray-800">{food.deliveryTime || '15 - 20 phút'}</span>
            </div>
          </div>

          {/* Action Button */}
          <button
            onClick={handleOrderNow}
            disabled={displayRestaurant.status !== 'Active'}
            className={`w-full text-white font-bold py-5 rounded-[2rem] shadow-xl shadow-orange-100 flex items-center justify-center gap-3 transform active:scale-95 transition-all ${displayRestaurant.status === 'Active' ? 'bg-[#EE501C] hover:bg-[#d44719]' : 'bg-gray-300 cursor-not-allowed shadow-none'}`}
          >
            <ShoppingCart className="w-6 h-6" />
            <span>{displayRestaurant.status === 'Active' ? `Đặt ngay • ${formatNumber(totalPrice)}đ` : 'Nhà hàng đang đóng cửa'}</span>
          </button>
        </div>
      </div>

      {/* Reviews Summary - Make Clickable */}
      <section
        onClick={handleViewReviews}
        className="bg-white rounded-[3rem] p-8 md:p-12 border border-gray-50 shadow-sm flex flex-col md:flex-row items-center justify-between gap-8 cursor-pointer hover:shadow-md transition-shadow group"
      >
        <div className="flex items-center gap-6">
          {/* Synchronized Average Rating */}
          <div className="text-5xl font-black text-gray-900 group-hover:text-[#EE501C] transition-colors">{averageRating}</div>
          <div>
            <div className="flex items-center gap-1 mb-1">
              {[...Array(5)].map((_, i) => (
                <Star key={i} className={`w-4 h-4 ${i < Math.round(Number(averageRating)) ? 'text-yellow-400 fill-yellow-400' : 'text-gray-200'}`} />
              ))}
            </div>
            {/* Synchronized Total Reviews */}
            <div className="text-xs font-bold text-gray-400 uppercase tracking-widest group-hover:text-gray-600">Dựa trên {totalReviews} lượt đánh giá</div>
          </div>
        </div>
        <button
          onClick={(e) => { e.stopPropagation(); handleViewReviews(); }}
          className="bg-orange-50 text-[#EE501C] font-bold px-10 py-4 rounded-2xl group-hover:bg-[#EE501C] group-hover:text-white transition-all flex items-center gap-2"
        >
          Xem chi tiết nhận xét <ChevronRight className="w-4 h-4" />
        </button>
      </section>

      {/* Vouchers */}
      <section>
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-2 text-gray-800 font-bold">
            <div className="w-5 h-5 bg-orange-100 rounded flex items-center justify-center">
              <div className="w-2.5 h-2.5 bg-[#EE501C] rounded-sm"></div>
            </div>
            Voucher khả dụng
          </div>
          <button
            onClick={handleViewAllVouchers}
            className="text-xs font-bold text-[#EE501C] flex items-center gap-1 hover:underline"
          >
            Xem tất cả <ChevronRight className="w-3 h-3" />
          </button>
        </div>
        <div className="flex gap-4 overflow-x-auto scrollbar-hide pb-4">
          {vouchers.length === 0 ? (
            <div className="text-center py-8 bg-gray-50 rounded-[2rem] border border-dashed border-gray-200 w-full">
              <p className="text-gray-400 text-sm">Hiện tại không có voucher khả dụng.</p>
            </div>
          ) : (
            vouchers.map((v) => {
              const isEligible = totalPrice >= v.minOrderValue;
              const missingAmount = v.minOrderValue - totalPrice;

              return (
                <div key={v.id} className={`min-w-[280px] md:min-w-[320px] bg-white border rounded-3xl p-4 flex gap-4 items-center shadow-sm relative group transition-all cursor-pointer ${isEligible ? 'border-gray-100 hover:border-orange-200' : 'border-gray-100 opacity-70 bg-gray-50'}`}>
                  <div className={`w-14 h-14 shrink-0 rounded-2xl flex items-center justify-center text-white ${v.type === 'FreeShip' ? 'bg-[#EE501C]' : 'bg-orange-300'} shadow-md ${!isEligible && 'grayscale'}`}>
                    {v.type === 'FreeShip' ? '🚢' : '%'}
                  </div>
                  <div className="flex-1">
                    <h4 className="text-sm font-bold text-gray-800">{v.title}</h4>
                    <p className="text-[10px] text-gray-400 mb-2">{v.condition}</p>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        if (isEligible) handleApplyVoucher(v);
                      }}
                      disabled={!isEligible}
                      className={`text-[10px] font-bold uppercase px-2 py-1 rounded transition-colors flex items-center gap-1 ${isEligible ? 'text-[#EE501C] hover:bg-orange-50' : 'text-gray-400 bg-gray-100 cursor-not-allowed'}`}
                    >
                      {isEligible ? 'Áp dụng' : (
                        <span className="flex items-center gap-1">
                          <Lock className="w-3 h-3" /> Thiếu {formatNumber(missingAmount / 1000)}k
                        </span>
                      )}
                    </button>
                  </div>
                  {isEligible && (
                    <div className="absolute right-4 top-1/2 -translate-y-1/2 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none">
                      <ChevronRight className="w-4 h-4 text-[#EE501C]" />
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      </section>

      {/* Related Foods (From Same Restaurant) */}
      <section id="related-foods">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-bold text-gray-800">Món ngon khác của quán</h2>
          {relatedFoods.length > 4 && (
            <div className="flex gap-2 items-center">
              <span className="text-sm text-gray-500">{currentPage + 1}/{totalPages}</span>
              <button 
                onClick={() => setCurrentPage(Math.max(0, currentPage - 1))}
                disabled={currentPage === 0}
                className="p-2 rounded-full border border-gray-200 hover:bg-gray-50 hover:border-[#EE501C] transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <ChevronLeft className="w-4 h-4 text-gray-400 hover:text-[#EE501C]" />
              </button>
              <button 
                onClick={() => setCurrentPage(Math.min(totalPages - 1, currentPage + 1))}
                disabled={currentPage === totalPages - 1}
                className="p-2 rounded-full border border-gray-200 hover:bg-gray-50 hover:border-[#EE501C] transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <ChevronRight className="w-4 h-4 text-gray-400 hover:text-[#EE501C]" />
              </button>
            </div>
          )}
        </div>

        {relatedFoods.length > 0 ? (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 md:gap-6">
            {displayedFoods.map((f) => {
              // Check if coming from checkout
              const locationState = location.state as any;
              const isFromCheckout = locationState?.fromCheckout && locationState?.currentCartItems;

              const handleRelatedFoodClick = async () => {
                // Click vào món → xem chi tiết
                if (isFromCheckout) {
                  // Navigate to product detail with checkout context
                  navigate(`/product/${f.id}`, {
                    state: { fromCheckout: true, currentCartItems: locationState.currentCartItems }
                  });
                } else {
                  // Normal flow: navigate to product detail
                  navigate(`/product/${f.id}`);
                }
              };

              const handleAddToCheckout = (e: React.MouseEvent) => {
                // Ngăn event bubble lên parent (không trigger handleRelatedFoodClick)
                e.stopPropagation();
                
                if (isFromCheckout) {
                  // Thêm món vào checkout mà không chuyển trang
                  const existingItems = locationState.currentCartItems || [];
                  const existingIndex = existingItems.findIndex((item: any) => item.food.id === f.id);

                  let updatedItems;
                  if (existingIndex >= 0) {
                    // Món đã có: CỘNG thêm 1 và chuyển lên đầu
                    updatedItems = [...existingItems];
                    updatedItems[existingIndex].quantity += 1; // CỘNG thêm 1
                    const item = updatedItems.splice(existingIndex, 1)[0];
                    updatedItems.unshift(item);
                  } else {
                    // Món mới: thêm vào đầu danh sách với số lượng = 1
                    updatedItems = [{ food: f, quantity: 1 }, ...existingItems];
                  }

                  // Cập nhật state và navigate lại checkout với items mới
                  navigate('/checkout', { 
                    state: { items: updatedItems },
                    replace: true // Thay thế history để không tạo entry mới
                  });
                } else {
                  // Flow bình thường: chuyển đến trang đặt món
                  navigate('/checkout', { state: { food: f, quantity: 1 } });
                }
              };

              return (
                <div
                  key={f.id}
                  onClick={handleRelatedFoodClick}
                  className="bg-white rounded-[1.5rem] md:rounded-[2rem] overflow-hidden border border-gray-100 shadow-sm hover:shadow-lg transition-all cursor-pointer group"
                >
                  <div className="h-32 md:h-44 relative overflow-hidden">
                    <img src={f.imageUrl} alt={f.name} className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-500" />
                  </div>
                  <div className="p-3 md:p-4">
                    <h4 className="font-bold text-gray-800 truncate mb-1 text-sm md:text-base">{f.name}</h4>
                    <div className="flex items-center gap-1 text-[10px] text-[#EE501C] font-bold mb-2 md:mb-3">
                      <Star className={`w-3 h-3 ${f.rating > 0 ? 'fill-[#EE501C]' : 'fill-none text-gray-300'}`} /> {f.rating > 0 ? f.rating.toFixed(1) : '0.0'} <span className="text-gray-300 font-medium">({f.reviewCount || 0}+)</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-base md:text-lg font-black text-[#EE501C]">{formatNumber(f.price)}đ</span>
                      <button
                        onClick={handleAddToCheckout}
                        className="w-7 h-7 md:w-8 md:h-8 rounded-lg md:rounded-xl bg-orange-50 flex items-center justify-center text-[#EE501C] hover:bg-[#EE501C] hover:text-white transition-colors"
                      >
                        <Plus className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="text-center py-10 bg-gray-50 rounded-[2rem] border border-dashed border-gray-200">
            <p className="text-gray-400 text-sm">Quán này chưa có món ăn khác.</p>
          </div>
        )}
      </section>

      {/* Login Request Modal */}
      <LoginRequestModal
        isOpen={showLoginModal}
        onClose={() => setShowLoginModal(false)}
      />

      {/* Floating Cart Button */}
      {cartItems.length > 0 && (
        <div className="fixed bottom-6 right-6 z-50">
          <button
            onClick={() => navigate('/checkout', { state: { items: cartItems } })}
            className="bg-[#EE501C] text-white px-6 py-4 rounded-full shadow-2xl hover:bg-[#d44719] transition-all flex items-center gap-3 group animate-in fade-in slide-in-from-bottom-5 duration-500"
          >
            <div className="relative">
              <ShoppingCart className="w-6 h-6" />
              {cartItemCount > 0 && (
                <span className="absolute -top-2 -right-2 bg-white text-[#EE501C] text-xs font-bold w-5 h-5 rounded-full flex items-center justify-center border-2 border-[#EE501C]">
                  {cartItemCount}
                </span>
              )}
            </div>
            <div className="flex flex-col items-start">
              <span className="text-xs font-medium opacity-90">Giỏ hàng</span>
              <span className="text-sm font-bold">{formatNumber(cartTotal)}đ</span>
            </div>
          </button>
        </div>
      )}
    </div>
  );
};
