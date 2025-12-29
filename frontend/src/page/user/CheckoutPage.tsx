
import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { MapPin, Ticket, CreditCard, ChevronRight, CheckCircle2, Wallet, Search, Plus, Loader2, X } from 'lucide-react';
import { FoodItem, UserProfile, Voucher } from '../../types/common';
import { getAvailableVouchersForUserApi } from '../../api/voucherApi';
import { createOrderApi } from '../../api/orderApi';
import { getUserProfileApi } from '../../api/userApi';
import { useAuthContext } from '../../contexts/AuthContext';
import { PasswordModal } from '../../components/common/PasswordModal';
import { OrderSuccessModal } from '../../components/user/OrderSuccessModal';
import { formatPhone, formatNumber, extractErrorMessage, isVoucherError } from '../../utils';

const DEFAULT_FOOD: FoodItem = {
  id: '1',
  name: 'Bún Bò Huế Đặc Biệt',
  price: 55000,
  imageUrl: 'https://images.unsplash.com/photo-1582878826618-c05326eff935?q=80&w=200&auto=format&fit=crop',
  rating: 4.8,
  description: '',
  category: 'noodles',
  restaurantId: 'res-1'
};

export const CheckoutPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user } = useAuthContext();
  const state = location.state as {
    food?: FoodItem,
    quantity?: number,
    voucher?: Voucher,
    items?: Array<{ food: FoodItem, quantity: number }>
  } | undefined;

  // Cart items state - support multiple items
  const [cartItems, setCartItems] = useState<Array<{ food: FoodItem, quantity: number }>>(() => {
    if (state?.items && Array.isArray(state.items) && state.items.length > 0) {
      return state.items;
    } else if (state?.food) {
      return [{ food: state.food, quantity: state.quantity || 1 }];
    }
    return [{ food: DEFAULT_FOOD, quantity: 1 }];
  });

  useEffect(() => {
    if (state?.items && Array.isArray(state.items) && state.items.length > 0) {
      setCartItems(state.items);
    }
  }, [state?.items]);

  const restaurantId = cartItems[0]?.food?.restaurantId || 'res-1';

  // User profile state
  const [userProfile, setUserProfile] = useState<UserProfile>({
    name: user?.name || 'Khách hàng',
    email: user?.email || 'email@example.com',
    phone: '(+84) 901 234 567',
    address: '123 Đường ABC, Phường Bến Nghé, Quận 1, Thành phố Hồ Chí Minh.',
    balance: 0,
    password: 'password123'
  });

  // Fetch user profile on mount
  useEffect(() => {
    const fetchUserProfile = async () => {
      if (user?.id) {
        try {
          const profile = await getUserProfileApi();
          setUserProfile({
            name: profile.fullname || 'Khách hàng',
            email: profile.email || '',
            phone: formatPhone(profile.phone_number),
            address: profile.address || 'Đang cập nhật địa chỉ',
            balance: profile.balance || 0,
            password: 'password123'
          });
        } catch (error) {
          console.error('Error fetching user profile:', error);
        }
      }
    };
    fetchUserProfile();
  }, [user?.id]);

  const subtotal = cartItems.reduce((sum, item) => sum + (item.food.price * item.quantity), 0);
  const initialVoucher = state?.voucher && subtotal >= state.voucher.minOrderValue ? state.voucher : null;

  const [paymentMethod, setPaymentMethod] = useState<'wallet' | 'cash'>('wallet');
  const [selectedVoucher, setSelectedVoucher] = useState<Voucher | null>(initialVoucher);
  const [availableVouchers, setAvailableVouchers] = useState<Voucher[]>([]);

  // Modals
  const [showPasswordModal, setShowPasswordModal] = useState(false);
  const [showSuccessModal, setShowSuccessModal] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);

  // Fetch vouchers
  useEffect(() => {
    const fetchVouchers = async () => {
      try {
        const data = await getAvailableVouchersForUserApi(restaurantId);
        setAvailableVouchers(data);
        if (!initialVoucher) {
          const valid = data.find(v => subtotal >= v.minOrderValue && !v.isExpired);
          if (valid) setSelectedVoucher(valid);
        }
      } catch (error) {
        console.error("Failed to fetch vouchers");
      }
    };
    fetchVouchers();
  }, [initialVoucher, subtotal, restaurantId]);

  const deliveryFee = 15000;

  // Calculate discount based on voucher type
  const calculateDiscount = () => {
    if (!selectedVoucher) return 0;

    switch (selectedVoucher.type) {
      case 'Percent':
        const percentDiscount = Math.round(subtotal * selectedVoucher.discountValue / 100);
        if (selectedVoucher.maxDiscount && percentDiscount > selectedVoucher.maxDiscount) {
          return selectedVoucher.maxDiscount;
        }
        return percentDiscount;
      case 'FreeShip':
        return Math.min(selectedVoucher.discountValue, deliveryFee);
      case 'Fixed':
      default:
        return selectedVoucher.discountValue;
    }
  };

  const discount = calculateDiscount();
  const total = Math.max(0, subtotal + deliveryFee - discount);

  // Cart management
  const removeItemFromCart = (foodId: string) => {
    setCartItems(prev => prev.filter(item => item.food.id !== foodId));
  };

  // Submit order
  const submitOrder = async (skipVoucher: boolean = false) => {
    if (cartItems.length === 0) {
      alert('Vui lòng chọn ít nhất một món ăn');
      return;
    }

    setIsProcessing(true);

    const promoId = skipVoucher ? null : (selectedVoucher?.id || null);
    const payload = {
      restaurantId: restaurantId,
      items: cartItems.map(item => ({
        food_name: item.food.name,
        quantity: item.quantity
      })),
      address: userProfile.address,
      note: '',
      payment_method: paymentMethod === 'wallet' ? 'Balance' : 'COD',
      shipping_fee: deliveryFee,
      promoId: promoId,
    };

    try {
      await createOrderApi(payload as any);

      try {
        const updatedVouchers = await getAvailableVouchersForUserApi(restaurantId);
        setAvailableVouchers(updatedVouchers);
        if (selectedVoucher && !updatedVouchers.find(v => v.id === selectedVoucher.id)) {
          setSelectedVoucher(null);
        }
      } catch (voucherError) {
        console.error("Failed to refresh vouchers:", voucherError);
      }

      setShowSuccessModal(true);
      setIsProcessing(false);
    } catch (error) {
      const originalMessage = error instanceof Error ? error.message : String(error);
      const hadVoucher = !skipVoucher && !!payload.promoId;
      const isVoucherErr = isVoucherError(originalMessage);

      if (isVoucherErr && hadVoucher) {
        setIsProcessing(false);
        const errorMessage = extractErrorMessage(error);
        alert(errorMessage + '\n\nVui lòng chọn voucher khác hoặc tiếp tục không có voucher.');
        setSelectedVoucher(null);
        return;
      } else {
        const errorMessage = extractErrorMessage(error);
        alert(errorMessage);
        setIsProcessing(false);
      }
    }
  };

  const handleConfirmClick = () => {
    if (paymentMethod === 'cash') {
      submitOrder();
    } else {
      if (userProfile.balance < total) {
        alert('Số dư không đủ');
        return;
      }
      setShowPasswordModal(true);
    }
  };

  const handlePasswordConfirm = async (password: string) => {
    if (password.length > 0) {
      setShowPasswordModal(false);
      await submitOrder();
    }
  };

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-8 py-8 animate-in fade-in duration-500 relative bg-white pb-24">
      {/* Breadcrumb */}
      <nav className="text-xs font-medium text-gray-400 flex items-center gap-2 mb-8 select-none">
        <button onClick={() => navigate('/')} className="hover:text-[#EE501C] transition-colors">Trang chủ</button>
        <ChevronRight className="w-3 h-3" />
        <button onClick={() => navigate(-1)} className="hover:text-[#EE501C] transition-colors">Món yêu thích</button>
        <ChevronRight className="w-3 h-3" />
        <span className="text-gray-800 font-semibold">Chi tiết thanh toán</span>
      </nav>

      <h1 className="text-2xl font-black text-gray-800 mb-8">Xác nhận thanh toán</h1>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-8 items-start">
        {/* LEFT COLUMN */}
        <div className="lg:col-span-7 space-y-6">
          {/* Address */}
          <div className="bg-white rounded-3xl p-6 border-2 border-gray-200 shadow-sm">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2 text-[#EE501C] font-bold">
                <MapPin className="w-5 h-5" /> Địa chỉ nhận hàng
              </div>
              <button
                onClick={() => navigate('/profile', { state: { view: 'EDIT_PROFILE' } })}
                className="text-xs font-bold text-[#EE501C] hover:underline"
              >
                Thay đổi
              </button>
            </div>
            <div className="space-y-1">
              <p className="font-bold text-gray-800">{userProfile.name} <span className="text-gray-400 font-medium ml-2">• {userProfile.phone}</span></p>
              <p className="text-sm text-gray-500">{userProfile.address}</p>
            </div>
          </div>

          {/* Cart Items */}
          <div className="bg-white rounded-3xl p-6 border-2 border-gray-200 shadow-sm">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-2 text-[#EE501C] font-bold">
                <Ticket className="w-5 h-5" /> Món đã chọn
              </div>
              <button
                onClick={() => {
                  navigate(`/product/${cartItems[0]?.food?.id || ''}#related-foods`, {
                    state: { fromCheckout: true, currentCartItems: cartItems, restaurantId }
                  });
                }}
                className="text-xs font-bold flex items-center gap-1.5 text-gray-500 hover:text-[#EE501C] hover:bg-orange-50 px-3 py-1.5 rounded-full transition-all"
              >
                <Plus className="w-3.5 h-3.5" /> Thêm món
              </button>
            </div>
            {cartItems.length === 0 ? (
              <div className="text-center py-8 text-gray-400">
                <p className="text-sm">Chưa có món nào được chọn</p>
              </div>
            ) : (
              <div className="space-y-4">
                {cartItems.map((item, index) => (
                  <div key={item.food.id} className={`flex gap-4 ${index > 0 ? 'pt-4 border-t border-gray-100' : ''}`}>
                    <div className="w-20 h-20 rounded-2xl overflow-hidden shrink-0 border border-gray-100 relative group">
                      <img src={item.food.imageUrl} alt={item.food.name} className="w-full h-full object-cover" />
                      <button
                        onClick={() => removeItemFromCart(item.food.id)}
                        className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center"
                        title="Bỏ món này"
                      >
                        <X className="w-6 h-6 text-white" />
                      </button>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-start justify-between gap-2">
                        <h3 className="font-bold text-gray-800 truncate flex-1">{item.food.name}</h3>
                        <button
                          onClick={() => removeItemFromCart(item.food.id)}
                          className="w-6 h-6 rounded-full bg-gray-100 hover:bg-red-100 flex items-center justify-center text-gray-400 hover:text-red-500 transition-colors shrink-0"
                          title="Bỏ món này"
                        >
                          <X className="w-3.5 h-3.5" />
                        </button>
                      </div>
                      <p className="text-xs text-gray-400 mb-2">Quán Ngon Nhà Làm</p>
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-bold text-[#EE501C]">{formatNumber(item.food.price)}đ</span>
                        <span className="text-sm font-bold text-gray-800 bg-gray-50 px-3 py-1 rounded-lg">x{item.quantity}</span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Payment Method */}
          <div className="bg-white rounded-3xl p-6 border-2 border-gray-200 shadow-sm">
            <div className="flex items-center gap-2 text-[#EE501C] font-bold mb-6">
              <CreditCard className="w-5 h-5" /> Phương thức thanh toán
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <button
                onClick={() => setPaymentMethod('wallet')}
                className={`flex items-center gap-3 p-4 rounded-2xl border-2 transition-all ${paymentMethod === 'wallet' ? 'border-[#EE501C] bg-orange-50' : 'border-gray-100 hover:border-gray-200'}`}
              >
                <div className="w-10 h-10 rounded-full bg-orange-100 flex items-center justify-center text-[#EE501C]">
                  <Wallet className="w-5 h-5" />
                </div>
                <div className="text-left flex-1">
                  <p className="text-sm font-bold text-gray-800">Ví điện tử</p>
                  <p className="text-[10px] text-gray-400">Số dư: <span className="text-[#EE501C] font-bold">{formatNumber(userProfile.balance)}đ</span></p>
                </div>
                {paymentMethod === 'wallet' && <CheckCircle2 className="w-5 h-5 text-[#EE501C] ml-auto" />}
              </button>

              <button
                onClick={() => setPaymentMethod('cash')}
                className={`flex items-center gap-3 p-4 rounded-2xl border-2 transition-all ${paymentMethod === 'cash' ? 'border-[#EE501C] bg-orange-50' : 'border-gray-100 hover:border-gray-200'}`}
              >
                <div className="w-10 h-10 rounded-full bg-gray-100 flex items-center justify-center text-gray-400">
                  <CreditCard className="w-5 h-5" />
                </div>
                <div className="text-left flex-1">
                  <p className="text-sm font-bold text-gray-800">Tiền mặt</p>
                  <p className="text-[10px] text-gray-400">Thanh toán khi nhận đồ</p>
                </div>
                {paymentMethod === 'cash' && <CheckCircle2 className="w-5 h-5 text-[#EE501C] ml-auto" />}
              </button>
            </div>
          </div>
        </div>

        {/* RIGHT COLUMN */}
        <div className="lg:col-span-5 space-y-6">
          <div className="bg-white rounded-3xl p-6 border-2 border-gray-200 shadow-sm sticky top-24">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2 text-[#EE501C] font-bold">
                <Ticket className="w-5 h-5" /> Ưu đãi & Voucher
              </div>
            </div>

            <div className="relative mb-6">
              <input type="text" placeholder="Nhập mã ưu đãi..." className="w-full bg-gray-50 border border-gray-100 rounded-xl py-3 pl-10 pr-4 text-sm focus:outline-none focus:ring-2 focus:ring-orange-100" />
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <button className="absolute right-2 top-1/2 -translate-y-1/2 bg-[#EE501C] text-white text-[10px] font-bold px-3 py-1.5 rounded-lg">Áp dụng</button>
            </div>

            <div className="space-y-4">
              <p className="text-xs font-bold text-gray-400 uppercase tracking-widest">Gợi ý cho bạn</p>
              {availableVouchers.filter(v => !v.isExpired).map(v => {
                const isFirstOrderEligible = v.eligibleFirstOrder !== false;
                const meetsMinOrder = subtotal >= v.minOrderValue;
                const isEligible = meetsMinOrder && isFirstOrderEligible;

                let ineligibilityReason = '';
                if (!meetsMinOrder) {
                  ineligibilityReason = 'Chưa đủ điều kiện';
                } else if (v.eligibleFirstOrder === false) {
                  ineligibilityReason = 'Chỉ áp dụng cho đơn đầu tiên';
                }

                return (
                  <div
                    key={v.id}
                    onClick={() => {
                      if (isEligible) {
                        if (selectedVoucher?.id === v.id) {
                          setSelectedVoucher(null);
                        } else {
                          setSelectedVoucher(v);
                        }
                      }
                    }}
                    className={`p-4 rounded-2xl border transition-all relative ${isEligible ? 'cursor-pointer hover:border-orange-100' : 'cursor-not-allowed opacity-60 bg-gray-50'
                      } ${selectedVoucher?.id === v.id ? 'border-orange-200 bg-orange-50/50' : 'border-gray-100'}`}
                  >
                    <div className="flex items-center gap-4">
                      <div className={`w-12 h-12 rounded-xl flex items-center justify-center text-white font-bold ${v.type === 'FreeShip' ? 'bg-[#EE501C]' : 'bg-orange-300'}`}>
                        {v.type === 'FreeShip' ? '🚢' : '%'}
                      </div>
                      <div className="flex-1">
                        <p className="text-xs font-bold text-gray-800">{v.title}</p>
                        <p className="text-[10px] text-gray-400">{v.condition}</p>
                        {!isEligible && ineligibilityReason && (
                          <p className="text-[10px] text-red-500 font-bold mt-1">{ineligibilityReason}</p>
                        )}
                      </div>
                      <div className="text-right">
                        <div className={`w-5 h-5 rounded-full border-2 flex items-center justify-center ${selectedVoucher?.id === v.id ? 'border-[#EE501C]' : 'border-gray-200'}`}>
                          {selectedVoucher?.id === v.id && <div className="w-2.5 h-2.5 rounded-full bg-[#EE501C]" />}
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>

            <div className="mt-6 pt-6 border-t border-gray-100 space-y-4">
              <div className="flex justify-between text-sm">
                <span className="text-gray-400">Tạm tính ({cartItems.reduce((sum, item) => sum + item.quantity, 0)} món)</span>
                <span className="font-bold text-gray-800">{formatNumber(subtotal)}đ</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-gray-400">Phí giao hàng</span>
                <span className="font-bold text-gray-800">{formatNumber(deliveryFee)}đ</span>
              </div>
              {selectedVoucher && (
                <div className="flex justify-between text-sm animate-in fade-in slide-in-from-right-4">
                  <span className="text-[#EE501C] font-bold">Voucher: {selectedVoucher.code}</span>
                  <span className="font-bold text-[#EE501C]">-{formatNumber(discount)}đ</span>
                </div>
              )}
              <div className="flex justify-between items-end pt-2 border-t border-gray-50 mt-2">
                <span className="font-bold text-gray-800">Tổng thanh toán</span>
                <span className="text-3xl font-black text-[#EE501C]">{formatNumber(total)}đ</span>
              </div>

              <button
                onClick={handleConfirmClick}
                disabled={(paymentMethod === 'wallet' && userProfile.balance < total) || isProcessing}
                className={`w-full text-white font-bold py-4 rounded-2xl shadow-xl transform active:scale-95 transition-all mt-4 flex items-center justify-center gap-2 ${(paymentMethod === 'wallet' && userProfile.balance < total) || isProcessing
                  ? 'bg-gray-300 cursor-not-allowed shadow-none'
                  : 'bg-[#EE501C] hover:bg-[#d44719] shadow-orange-100'
                  }`}
              >
                {isProcessing ? (
                  <>
                    <Loader2 className="w-5 h-5 animate-spin" /> Đang xử lý...
                  </>
                ) : (
                  paymentMethod === 'wallet' && userProfile.balance < total
                    ? 'Số dư không đủ'
                    : 'Xác nhận thanh toán'
                )}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Modals */}
      <PasswordModal
        isOpen={showPasswordModal}
        onClose={() => setShowPasswordModal(false)}
        onConfirm={handlePasswordConfirm}
        isProcessing={isProcessing}
      />

      <OrderSuccessModal isOpen={showSuccessModal} />
    </div>
  );
};