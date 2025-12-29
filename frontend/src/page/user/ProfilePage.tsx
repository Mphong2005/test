
import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  ChevronRight, User, Ticket, Heart, CreditCard, Wallet, LogOut,
  Truck, Mail, Phone, Calendar, MapPin,
  CreditCard as CardIcon, X
} from 'lucide-react';
import { FoodItem, ProfileSubPage, UserProfile, Voucher } from '../../types/common';
import { useAuthContext } from '../../contexts/AuthContext';
import { getAvailableVouchersForUserApi, getExpiredVouchersForUserApi } from '../../api/voucherApi';
import { getFoodsApi } from '../../api/productApi';
import { getUserProfileApi, updateUserApi, topUpBalanceApi } from '../../api/userApi';
import { getCartApi, removeFromCartApi } from '../../api/cartApi';
import { formatDateISO, formatDateVN, formatNumber, extractErrorMessage } from '../../utils';
import { PasswordModal } from '../../components/common/PasswordModal';

const ProfileItem = ({
  icon: Icon,
  title,
  subtitle,
  badge,
  action,
  isDanger,
  onClick
}: {
  icon: any,
  title: string,
  subtitle?: string,
  badge?: string,
  action?: React.ReactNode,
  isDanger?: boolean,
  onClick?: () => void
}) => (
  <div
    onClick={onClick}
    className={`bg-white rounded-2xl p-4 flex items-center gap-4 border border-gray-50 shadow-sm hover:shadow-md transition-all cursor-pointer group ${isDanger ? 'hover:border-red-100' : 'hover:border-orange-100'}`}
  >
    <div className={`w-12 h-12 rounded-2xl flex items-center justify-center relative ${isDanger ? 'bg-red-50 text-red-500' : 'bg-orange-50 text-[#EE501C]'}`}>
      <Icon className="w-6 h-6" />
      {badge && (
        <span className="absolute -top-1 -right-1 bg-red-500 text-white text-[10px] font-bold w-4 h-4 rounded-full flex items-center justify-center border-2 border-white">
          {badge}
        </span>
      )}
    </div>
    <div className="flex-1">
      <h4 className={`text-sm font-bold ${isDanger ? 'text-red-600' : 'text-gray-800'} group-hover:text-[#EE501C] transition-colors`}>{title}</h4>
      {subtitle && <p className="text-[10px] text-gray-400 mt-0.5">{subtitle}</p>}
    </div>
    {action ? action : <ChevronRight className="w-4 h-4 text-gray-300 group-hover:text-[#EE501C]" />}
  </div>
);

export const ProfilePage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout } = useAuthContext();
  const [subPage, setSubPage] = useState<ProfileSubPage>('MAIN');
  const [paymentMethod, setPaymentMethod] = useState<'APP' | 'COD'>('COD');
  const [voucherTab, setVoucherTab] = useState<'AVAILABLE' | 'EXPIRED'>('AVAILABLE');
  const [gender, setGender] = useState<'male' | 'female' | 'other'>('male');

  // Data State
  const [userProfile, setUserProfile] = useState<UserProfile>({
    name: '',
    email: '',
    phone: '',
    address: '',
    balance: 0
  });
  const [dob, setDob] = useState<string>('');

  const [vouchers, setVouchers] = useState<Voucher[]>([]);
  const [favoriteFoods, setFavoriteFoods] = useState<FoodItem[]>([]);
  const [loading, setLoading] = useState(true);

  // Modal states
  const [showDepositModal, setShowDepositModal] = useState(false);
  const [showPasswordModal, setShowPasswordModal] = useState(false);
  const [depositAmount, setDepositAmount] = useState<string>('');

  // Local form state for editing
  const [formName, setFormName] = useState('');
  const [formEmail, setFormEmail] = useState('');
  const [formPhone, setFormPhone] = useState('');
  const [formAddress, setFormAddress] = useState('');

  // Handle incoming navigation state (e.g. from ProductDetail "See All Vouchers")
  useEffect(() => {
    if (location.state && location.state.view) {
      setSubPage(location.state.view as ProfileSubPage);
      // Clear state to prevent stuck navigation if needed, 
      // though strictly not necessary if we only read on mount/update
    }
  }, [location.state]);

  // Fetch data - chỉ chạy khi component mount hoặc user.id thay đổi
  useEffect(() => {
    // Chỉ fetch khi có user và user.id hợp lệ
    if (!user?.id) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      setLoading(true);
      try {
        // 1. Fetch Profile (uses current authenticated user)
        const profileData = await getUserProfileApi();
        setUserProfile({
          name: profileData.fullname,
          email: profileData.email,
          phone: profileData.phone_number || '',
          address: profileData.address || '',
          balance: profileData.balance || 0
        });
        setDob(formatDateISO(profileData.birthday));
        setGender((profileData.gender === 'Male' ? 'male' : profileData.gender === 'Female' ? 'female' : 'other'));

        // Init form data
        setFormName(profileData.fullname);
        setFormEmail(profileData.email);
        setFormPhone(profileData.phone_number || '');
        setFormAddress(profileData.address || '');

        // 2. Fetch Vouchers & Favorites in parallel
        try {
          // Fetch both available and expired vouchers
          const [availableVouchers, expiredVouchers] = await Promise.all([
            getAvailableVouchersForUserApi(),
            getExpiredVouchersForUserApi()
          ]);
          // Merge both arrays
          setVouchers([...availableVouchers, ...expiredVouchers]);
        } catch (error) {
          console.error('Error fetching vouchers:', error);
          setVouchers([]);
        }

        // Load favorites from cart
        try {
          const cart = await getCartApi();
          if (cart && cart.items && cart.items.length > 0) {
            // Transform cart items to FoodItem format
            const favorites: FoodItem[] = [];
            // Fetch all foods ONCE outside the loop
            const allFoods = await getFoodsApi();

            for (const item of cart.items) {
              const foodId = `${item.restaurantId}-${item.foodName}`;
              const food = allFoods.find(f => f.id === foodId);

              if (food) {
                favorites.push(food);
              } else {
                // If food not found in all foods, create a basic FoodItem from cart item
                favorites.push({
                  id: foodId,
                  name: item.foodName,
                  price: item.unitPrice,
                  description: '',
                  imageUrl: '',
                  category: '',
                  restaurantId: item.restaurantId,
                  rating: 4.0,
                  distance: '1.5',
                  deliveryTime: '15-20 phút',
                });
              }
            }
            setFavoriteFoods(favorites);
          } else {
            setFavoriteFoods([]);
          }
        } catch (error) {
          console.error('Error fetching favorites from cart:', error);
          setFavoriteFoods([]);
        }
      } catch (error) {
        console.error("Error fetching profile", error);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [user?.id]); // Chỉ phụ thuộc vào user.id thay vì toàn bộ user object

  const handleSaveProfile = async () => {
    try {
      // Map gender from frontend format to backend format
      const genderMap: Record<string, 'Male' | 'Female' | undefined> = {
        'male': 'Male',
        'female': 'Female',
        'other': undefined
      };

      await updateUserApi({
        fullname: formName,
        email: formEmail,
        phone_number: formPhone,
        address: formAddress,
        birthday: dob ? new Date(dob).toISOString() : undefined,
        gender: genderMap[gender]
      });

      // Update local state
      setUserProfile(prev => ({
        ...prev,
        name: formName,
        email: formEmail,
        phone: formPhone,
        address: formAddress
      }));

      alert('Cập nhật thành công!');
      setSubPage('MAIN');
    } catch (error: unknown) {
      alert(extractErrorMessage(error));
    }
  };

  const onLogout = () => {
    logout();
    navigate('/login');
  };

  const onToggleFavorite = async (id: string) => {
    // Update local state
    setFavoriteFoods(prev => prev.filter(f => f.id !== id));

    // Remove from cart (favorites) in backend
    try {
      // Extract food name from food.id format: restaurantId-foodName
      const foodName = id.includes('-') ? id.split('-').slice(1).join('-') : '';
      if (foodName) {
        await removeFromCartApi(foodName);
      }
    } catch (error: any) {
      console.error('Failed to remove from favorites:', error);
      // Revert local state on error
      const food = await getFoodsApi();
      const cart = await getCartApi();
      if (cart && cart.items) {
        const favorites: FoodItem[] = [];
        for (const item of cart.items) {
          const foodId = `${item.restaurantId}-${item.foodName}`;
          const foundFood = food.find(f => f.id === foodId);
          if (foundFood) {
            favorites.push(foundFood);
          }
        }
        setFavoriteFoods(favorites);
      }
      alert(extractErrorMessage(error));
    }
  };

  const onOrderNow = (item: FoodItem) => {
    navigate(`/checkout`, { state: { food: item, quantity: 1 } });
  };

  // Modal handlers
  const handleOpenDepositModal = () => {
    setDepositAmount('');
    setShowDepositModal(true);
  };

  const handleDepositConfirm = () => {
    if (!depositAmount || parseInt(depositAmount) <= 0) {
      alert('Vui lòng nhập số tiền hợp lệ!');
      return;
    }
    setShowDepositModal(false);
    setShowPasswordModal(true);
  };

  const handlePasswordConfirm = async (password: string) => {
    try {
      const amount = parseFloat(depositAmount);
      if (isNaN(amount) || amount <= 0) {
        alert('Vui lòng nhập số tiền hợp lệ!');
        return;
      }

      // Call API to top up balance
      const updatedUser = await topUpBalanceApi(amount);

      // Update local state with new balance
      setUserProfile(prev => ({
        ...prev,
        balance: updatedUser.balance || prev.balance + amount
      }));

      setShowPasswordModal(false);
      setDepositAmount('');
      alert(`Nạp tiền thành công! Số dư mới: ${formatNumber(updatedUser.balance || userProfile.balance + amount)}đ`);
    } catch (error: unknown) {
      alert(extractErrorMessage(error));
      setShowPasswordModal(false);
    }
  };

  // Helper function to check voucher validity dynamically
  const isVoucherValid = (v: Voucher) => {
    const now = new Date();
    // 1. Check Status
    if (v.status === 'Expired' || v.status === 'Inactive') return false;
    if (v.isExpired) return false;

    // 2. Check Date
    if (v.endDate) {
      const end = new Date(v.endDate);
      // Set time to end of day to be generous
      end.setHours(23, 59, 59, 999);
      if (end < now) return false;
    }

    return true;
  };

  const validVoucherCount = vouchers.filter(isVoucherValid).length;

  const renderBreadcrumb = () => {
    const pages = [
      { name: 'Trang chủ', onClick: () => navigate('/') },
      { name: 'Hồ sơ người dùng', onClick: () => setSubPage('MAIN') }
    ];

    if (subPage === 'PAYMENT') pages.push({ name: 'Phương thức thanh toán', onClick: () => { } });
    if (subPage === 'FAVORITES') pages.push({ name: 'Món yêu thích', onClick: () => { } });
    if (subPage === 'VOUCHERS') pages.push({ name: 'Ví vouchers', onClick: () => { } });
    if (subPage === 'EDIT_PROFILE') pages.push({ name: 'Đổi thông tin cá nhân', onClick: () => { } });

    return (
      <nav className="text-xs font-medium text-gray-400 flex items-center gap-2 mb-10 select-none">
        {pages.map((p, idx) => (
          <React.Fragment key={p.name}>
            <button
              onClick={p.onClick}
              className={`${idx === pages.length - 1 ? 'text-gray-800 font-semibold cursor-default' : 'hover:text-[#EE501C] transition-colors cursor-pointer'}`}
            >
              {p.name}
            </button>
            {idx < pages.length - 1 && <ChevronRight className="w-3 h-3 text-gray-400" />}
          </React.Fragment>
        ))}
      </nav>
    );
  };

  const renderUserCard = () => (
    <div className="bg-white rounded-[2.5rem] p-10 border border-gray-50 shadow-sm text-center flex flex-col items-center sticky top-24">
      <div className="w-40 h-40 rounded-full overflow-hidden border-8 border-orange-50 mb-6 shadow-inner bg-orange-50">
        <img
          src={`https://ui-avatars.com/api/?name=${encodeURIComponent(userProfile.name)}&background=fff7ed&color=ee501c&bold=true&size=200`}
          alt="User Avatar"
          className="w-full h-full object-cover"
        />
      </div>
      <h2 className="text-2xl font-black text-gray-900 mb-1">{userProfile.name}</h2>
      <p className="text-sm text-gray-400 font-medium">{userProfile.email}</p>
    </div>
  );

  const renderPaymentView = () => (
    <div className="space-y-8 animate-in fade-in slide-in-from-right-4 duration-300 border border-gray-200 rounded-3xl p-6 bg-white shadow-sm">
      <h3 className="text-2xl font-bold text-gray-900">Phương thức thanh toán</h3>
      <div className="space-y-6">
        {/* APP PAYMENT */}
        <div
          onClick={() => setPaymentMethod('APP')}
          className={`bg-white rounded-[2.5rem] p-8 border-2 transition-all cursor-pointer flex gap-6 items-start ${paymentMethod === 'APP' ? 'border-[#EE501C] shadow-md' : 'border-gray-50 hover:border-orange-100'}`}
        >
          <div className="w-14 h-14 rounded-2xl bg-orange-50 flex items-center justify-center text-[#EE501C] shrink-0">
            <Wallet className="w-7 h-7" />
          </div>
          <div className="flex-1">
            <h4 className="text-lg font-bold text-gray-900 mb-2">Thanh toán trực tiếp qua ứng dụng</h4>
            <p className="text-sm text-gray-400 leading-relaxed mb-4">Người dùng nạp tiền vào trang web và sử dụng số dư này để thanh toán các đơn hàng.</p>
            <div className="flex flex-wrap gap-2">
              {['Momo', 'ZaloPay', 'Visa/Mastercard'].map(tag => (
                <span key={tag} className="bg-gray-50 text-[10px] font-bold text-gray-500 px-4 py-1.5 rounded-full border border-gray-100">{tag}</span>
              ))}
            </div>
          </div>
        </div>

        {/* COD PAYMENT */}
        <div
          onClick={() => setPaymentMethod('COD')}
          className={`bg-white rounded-[2.5rem] p-8 border-2 transition-all cursor-pointer flex gap-6 items-start ${paymentMethod === 'COD' ? 'border-[#EE501C] shadow-md' : 'border-gray-50 hover:border-orange-100'}`}
        >
          <div className="w-14 h-14 rounded-2xl bg-[#EE501C] flex items-center justify-center text-white shrink-0">
            <CardIcon className="w-7 h-7" />
          </div>
          <div className="flex-1">
            <h4 className="text-lg font-bold text-gray-900 mb-2">Thanh toán khi nhận hàng (COD)</h4>
            <p className="text-sm text-gray-400 leading-relaxed">Thanh toán bằng tiền mặt trực tiếp cho tài xế khi bạn nhận được món ăn của mình.</p>
          </div>
        </div>
      </div>

      <div className="flex justify-end pt-10 mt-6 border-t border-gray-100 border-dashed">
        <button className="bg-[#EE501C] text-white font-bold py-4 px-12 rounded-2xl shadow-xl shadow-orange-100 hover:bg-[#d44719] transform active:scale-95 transition-all text-sm">
          Lưu thay đổi
        </button>
      </div>
    </div>
  );

  const renderMainView = () => (
    <div className="space-y-10">
      <div className="border border-gray-200 rounded-3xl p-6 bg-white shadow-sm">
        <h3 className="text-xl font-bold text-gray-800 mb-6 px-2">Cài đặt tài khoản</h3>
        <div className="space-y-4">
          <ProfileItem
            icon={User}
            title="Đổi thông tin cá nhân"
            subtitle="Cập nhật tên, email, số điện thoại và địa chỉ"
            onClick={() => setSubPage('EDIT_PROFILE')}
          />
          <ProfileItem
            icon={Ticket}
            title="Ví vouchers"
            subtitle={`${validVoucherCount} mã giảm giá đang chờ bạn`}
            badge={validVoucherCount > 0 ? validVoucherCount.toString() : undefined}
            onClick={() => setSubPage('VOUCHERS')}
          />
          <ProfileItem
            icon={Heart}
            title="Món yêu thích"
            subtitle={`Bạn đang có ${favoriteFoods.length} món yêu thích`}
            badge={favoriteFoods.length > 0 ? favoriteFoods.length.toString() : undefined}
            onClick={() => setSubPage('FAVORITES')}
          />
          <ProfileItem
            icon={CreditCard}
            title="Phương thức thanh toán"
            subtitle="Quản lý thẻ ngân hàng và ví điện tử"
            onClick={() => setSubPage('PAYMENT')}
          />
          <ProfileItem
            icon={Wallet}
            title="Số dư tài khoản"
            subtitle={`${formatNumber(userProfile.balance)}đ`}
            action={
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  handleOpenDepositModal();
                }}
                className="bg-[#EE501C] text-white text-xs font-bold px-4 py-2 rounded-xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all"
              >
                Nạp tiền
              </button>
            }
          />
        </div>
      </div>
      <div className="pt-4">
        <button onClick={onLogout} className="w-full bg-white rounded-2xl p-5 border border-gray-50 shadow-sm hover:shadow-md hover:border-red-100 transition-all flex items-center justify-center gap-3 group">
          <LogOut className="w-5 h-5 text-red-500 group-hover:scale-110 transition-transform" />
          <span className="text-sm font-bold text-gray-800 group-hover:text-red-500 transition-colors">Đăng xuất tài khoản</span>
        </button>
      </div>
    </div>
  );

  const renderFavoritesView = () => (
    <div className="animate-in fade-in slide-in-from-right-4 duration-300 border border-gray-200 rounded-3xl p-6 bg-white shadow-sm">
      <h3 className="text-xl font-bold text-gray-800 mb-8 px-2">Món yêu thích ({favoriteFoods.length})</h3>
      {favoriteFoods.length > 0 ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
          {favoriteFoods.map((item) => (
            <div key={item.id} className="bg-white rounded-[2rem] border border-gray-50 shadow-sm p-4 hover:shadow-md transition-all group">
              <div className="relative h-40 mb-4 rounded-2xl overflow-hidden">
                <img src={item.imageUrl} alt={item.name} className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500" />
                <button
                  onClick={() => onToggleFavorite(item.id)}
                  className="absolute top-3 right-3 w-8 h-8 rounded-full bg-white shadow-sm flex items-center justify-center text-[#EE501C] hover:scale-110 transition-transform"
                >
                  <Heart className="w-4 h-4 fill-[#EE501C]" />
                </button>
              </div>
              <div className="flex justify-between items-start mb-1 px-1">
                <h4 className="font-bold text-gray-900 group-hover:text-[#EE501C] transition-colors">{item.name}</h4>
                <span className="text-[#EE501C] font-black">{formatNumber(item.price)}đ</span>
              </div>
              <button
                onClick={() => onOrderNow(item)}
                className="w-full mt-4 bg-orange-50 text-[#EE501C] font-bold py-2.5 rounded-xl text-xs hover:bg-[#EE501C] hover:text-white transition-all"
              >
                Đặt ngay
              </button>
            </div>
          ))}
        </div>
      ) : (
        <div className="text-center py-20 bg-gray-50 rounded-[2rem] border border-dashed border-gray-200">
          <p className="text-gray-400 font-medium italic">Bạn chưa có món ăn yêu thích nào.</p>
        </div>
      )}
    </div>
  );

  const renderVouchersView = () => {
    const displayedVouchers = vouchers.filter(v => {
      const valid = isVoucherValid(v);
      return voucherTab === 'AVAILABLE' ? valid : !valid;
    });

    return (
      <div className="space-y-8 animate-in fade-in slide-in-from-right-4 duration-300 border border-gray-200 rounded-3xl p-6 bg-white shadow-sm">
        <div>
          <h3 className="text-xl font-bold text-gray-800 mb-6 px-2">Ví vouchers</h3>
          <div className="relative flex gap-3 mb-8">
            <div className="flex-1 relative">
              <input
                type="text"
                placeholder="Nhập mã voucher"
                className="w-full bg-white border border-gray-100 rounded-2xl py-4 px-6 text-sm focus:ring-2 focus:ring-orange-100 focus:border-[#EE501C] outline-none shadow-sm transition-all"
              />
            </div>
            <button className="bg-[#EE501C] text-white font-bold py-4 px-10 rounded-2xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all">Lưu</button>
          </div>

          <div className="flex gap-10 border-b border-gray-100 mb-8 px-2">
            <button
              onClick={() => setVoucherTab('AVAILABLE')}
              className={`pb-4 text-sm font-bold relative transition-all ${voucherTab === 'AVAILABLE' ? 'text-[#EE501C]' : 'text-gray-400'}`}
            >
              Có thể dùng <span className="absolute -top-1 -right-2 w-1.5 h-1.5 bg-[#EE501C] rounded-full"></span>
              {voucherTab === 'AVAILABLE' && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-[#EE501C] rounded-full" />}
            </button>
            <button
              onClick={() => setVoucherTab('EXPIRED')}
              className={`pb-4 text-sm font-bold relative transition-all ${voucherTab === 'EXPIRED' ? 'text-[#EE501C]' : 'text-gray-400'}`}
            >
              Đã hết hạn
              {voucherTab === 'EXPIRED' && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-[#EE501C] rounded-full" />}
            </button>
          </div>

          <div className="space-y-4">
            {displayedVouchers.length > 0 ? displayedVouchers.map((v, idx) => {
              const isActuallyExpired = !isVoucherValid(v);
              return (
                <div
                  key={idx}
                  className={`bg-white border border-gray-50 rounded-[2rem] p-6 flex gap-6 items-center shadow-sm hover:shadow-md transition-all group relative overflow-hidden ${isActuallyExpired ? 'opacity-60' : ''}`}
                >
                  <div className={`w-20 h-20 rounded-2xl flex flex-col items-center justify-center shrink-0 ${v.type === 'FreeShip' ? 'bg-orange-50' : 'bg-orange-100'}`}>
                    {v.type === 'FreeShip' ? <Truck className={`w-8 h-8 ${isActuallyExpired ? 'text-gray-400' : 'text-[#EE501C]'}`} /> : <span className={`text-3xl ${isActuallyExpired ? 'text-gray-400' : 'text-[#EE501C]'}`}>%</span>}
                    <span className={`text-[8px] font-black uppercase mt-1 tracking-widest ${isActuallyExpired ? 'text-gray-400' : 'text-[#EE501C]'}`}>{v.type}</span>
                  </div>

                  <div className="flex-1">
                    <h4 className={`font-bold text-gray-900 mb-1 ${isActuallyExpired ? 'text-gray-500' : ''}`}>{v.title}</h4>
                    <p className="text-xs text-gray-400 mb-1">{v.condition}</p>
                    <div className="flex items-center gap-4 text-[10px] font-bold text-gray-400">
                      <span>Mã: {v.code}</span>
                      {v.endDate && <span>HSD: {formatDateVN(v.endDate)}</span>}
                    </div>
                  </div>

                  <div className="text-right">
                    <div className={`text-2xl font-black mb-2 ${isActuallyExpired ? 'text-gray-400' : 'text-[#EE501C]'}`}>
                      {v.type === 'Percent' ? `-${v.discountValue}%` : v.type === 'FreeShip' ? 'FREE' : `-${v.discountValue / 1000}k`}
                    </div>
                    <button
                      onClick={() => !isActuallyExpired && navigate('/')}
                      className={`text-[10px] font-black uppercase tracking-widest ${isActuallyExpired ? 'text-gray-400 cursor-default' : 'text-[#EE501C] hover:underline cursor-pointer'}`}
                    >
                      {isActuallyExpired ? 'Hết hạn' : 'Dùng ngay'}
                    </button>
                  </div>
                </div>
              )
            }) : (
              <div className="text-center py-10 bg-gray-50 rounded-[2rem] border border-dashed border-gray-200">
                <p className="text-gray-400 text-sm">Không có voucher nào trong mục này.</p>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  };

  const renderEditProfileView = () => (
    <div className="bg-white rounded-[2.5rem] p-8 md:p-10 border border-gray-50 shadow-sm animate-in fade-in slide-in-from-right-4 duration-300">
      <h3 className="text-xl font-bold text-gray-800 mb-6 pb-4 border-b border-gray-50">Cập nhật thông tin</h3>

      <div className="space-y-6">
        <div className="space-y-2">
          <label className="text-xs font-bold text-gray-400 ml-1">Họ và tên</label>
          <div className="relative">
            <User className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-300 w-5 h-5" />
            <input
              type="text"
              value={formName}
              onChange={(e) => setFormName(e.target.value)}
              className="w-full bg-gray-50 border border-transparent rounded-2xl py-4 pl-12 pr-4 text-sm font-medium focus:bg-white focus:border-orange-100 focus:ring-4 focus:ring-orange-50 outline-none transition-all text-gray-700"
            />
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="space-y-2">
            <label className="text-xs font-bold text-gray-400 ml-1">Email</label>
            <div className="relative">
              <Mail className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-300 w-5 h-5" />
              <input
                type="email"
                value={formEmail}
                onChange={(e) => setFormEmail(e.target.value)}
                className="w-full bg-gray-50 border border-transparent rounded-2xl py-4 pl-12 pr-4 text-sm font-medium focus:bg-white focus:border-orange-100 focus:ring-4 focus:ring-orange-50 outline-none transition-all text-gray-700"
              />
            </div>
          </div>
          <div className="space-y-2">
            <label className="text-xs font-bold text-gray-400 ml-1">Số điện thoại</label>
            <div className="relative">
              <Phone className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-300 w-5 h-5" />
              <input
                type="tel"
                value={formPhone}
                onChange={(e) => setFormPhone(e.target.value)}
                className="w-full bg-gray-50 border border-transparent rounded-2xl py-4 pl-12 pr-4 text-sm font-medium focus:bg-white focus:border-orange-100 focus:ring-4 focus:ring-orange-50 outline-none transition-all text-gray-700"
              />
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 items-end">
          <div className="space-y-2">
            <label className="text-xs font-bold text-gray-400 ml-1">Ngày sinh</label>
            <div className="relative">
              <Calendar className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-300 w-5 h-5" />
              <input
                type="date"
                defaultValue={dob}
                onChange={(e) => setDob(e.target.value)}
                className="w-full bg-gray-50 border border-transparent rounded-2xl py-4 pl-12 pr-4 text-sm font-medium focus:bg-white focus:border-orange-100 focus:ring-4 focus:ring-orange-50 outline-none transition-all text-gray-700"
              />
            </div>
          </div>
          <div className="space-y-2">
            <label className="text-xs font-bold text-gray-400 ml-1">Giới tính</label>
            <div className="flex items-center gap-6 h-12">
              <label className="flex items-center gap-2 cursor-pointer group">
                <div
                  onClick={() => setGender('male')}
                  className={`w-5 h-5 rounded-full border-2 flex items-center justify-center transition-all ${gender === 'male' ? 'border-[#EE501C]' : 'border-gray-200 group-hover:border-orange-200'}`}
                >
                  {gender === 'male' && <div className="w-2.5 h-2.5 rounded-full bg-[#EE501C]" />}
                </div>
                <span className={`text-sm font-bold ${gender === 'male' ? 'text-gray-800' : 'text-gray-400'}`}>Nam</span>
              </label>
              <label className="flex items-center gap-2 cursor-pointer group">
                <div
                  onClick={() => setGender('female')}
                  className={`w-5 h-5 rounded-full border-2 flex items-center justify-center transition-all ${gender === 'female' ? 'border-[#EE501C]' : 'border-gray-200 group-hover:border-orange-200'}`}
                >
                  {gender === 'female' && <div className="w-2.5 h-2.5 rounded-full bg-[#EE501C]" />}
                </div>
                <span className={`text-sm font-bold ${gender === 'female' ? 'text-gray-800' : 'text-gray-400'}`}>Nữ</span>
              </label>
            </div>
          </div>
        </div>

        <div className="space-y-2">
          <label className="text-xs font-bold text-gray-400 ml-1">Địa chỉ</label>
          <div className="relative">
            <MapPin className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-300 w-5 h-5" />
            <input
              type="text"
              value={formAddress}
              onChange={(e) => setFormAddress(e.target.value)}
              className="w-full bg-gray-50 border border-transparent rounded-2xl py-4 pl-12 pr-4 text-sm font-medium focus:bg-white focus:border-orange-100 focus:ring-4 focus:ring-orange-50 outline-none transition-all text-gray-700"
            />
          </div>
        </div>

        <div className="flex gap-4 pt-6">
          <button
            className="flex-1 bg-[#EE501C] text-white font-black py-4 rounded-2xl shadow-xl shadow-orange-100 hover:bg-[#d44719] transform active:scale-[0.98] transition-all"
            onClick={handleSaveProfile}
          >
            Lưu thay đổi
          </button>
          <button
            className="flex-1 bg-white border border-gray-200 text-gray-500 font-bold py-4 rounded-2xl hover:bg-gray-50 transition-all"
            onClick={() => setSubPage('MAIN')}
          >
            Hủy
          </button>
        </div>
      </div>
    </div>
  );

  if (loading) {
    return <div className="min-h-screen flex items-center justify-center">Đang tải hồ sơ...</div>;
  }

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-8 py-10 min-h-[80vh]">
      {renderBreadcrumb()}

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-12">
        {/* Left Column (Avatar/Basic Info) */}
        <div className="lg:col-span-4">
          {renderUserCard()}
        </div>

        {/* Right Column (Dynamic Sub-pages) */}
        <div className="lg:col-span-8">
          {subPage === 'MAIN' && renderMainView()}
          {subPage === 'PAYMENT' && renderPaymentView()}
          {subPage === 'FAVORITES' && renderFavoritesView()}
          {subPage === 'VOUCHERS' && renderVouchersView()}
          {subPage === 'EDIT_PROFILE' && renderEditProfileView()}
        </div>
      </div>

      {/* Deposit Modal */}
      {showDepositModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 animate-in fade-in duration-200">
          <div className="bg-white rounded-3xl p-8 w-full max-w-md mx-4 shadow-2xl animate-in zoom-in-95 duration-200">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-bold text-gray-900">Nạp tiền vào tài khoản</h3>
              <button
                onClick={() => setShowDepositModal(false)}
                className="w-8 h-8 rounded-full bg-gray-100 flex items-center justify-center hover:bg-gray-200 transition-colors"
              >
                <X className="w-4 h-4 text-gray-500" />
              </button>
            </div>

            <div className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-bold text-gray-600">Số tiền nạp (VNĐ)</label>
                <div className="relative">
                  <Wallet className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-400 w-5 h-5" />
                  <input
                    type="number"
                    value={depositAmount}
                    onChange={(e) => setDepositAmount(e.target.value)}
                    placeholder="Nhập số tiền..."
                    className="w-full bg-gray-50 border border-gray-200 rounded-2xl py-4 pl-12 pr-4 text-sm font-medium focus:bg-white focus:border-[#EE501C] focus:ring-4 focus:ring-orange-50 outline-none transition-all"
                  />
                </div>
              </div>

              {/* Quick amount buttons */}
              <div className="flex flex-wrap gap-2">
                {[50000, 100000, 200000, 500000].map((amount) => (
                  <button
                    key={amount}
                    onClick={() => setDepositAmount(amount.toString())}
                    className="px-4 py-2 bg-orange-50 text-[#EE501C] text-xs font-bold rounded-xl hover:bg-orange-100 transition-colors"
                  >
                    {formatNumber(amount)}đ
                  </button>
                ))}
              </div>
            </div>

            <div className="flex gap-4 mt-8">
              <button
                onClick={() => setShowDepositModal(false)}
                className="flex-1 bg-gray-100 text-gray-600 font-bold py-4 rounded-2xl hover:bg-gray-200 transition-all"
              >
                Huỷ bỏ
              </button>
              <button
                onClick={handleDepositConfirm}
                className="flex-1 bg-[#EE501C] text-white font-bold py-4 rounded-2xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all"
              >
                Xác nhận
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Password Confirmation Modal */}
      <PasswordModal
        isOpen={showPasswordModal}
        onClose={() => {
          setShowPasswordModal(false);
          setShowDepositModal(true);
        }}
        onConfirm={handlePasswordConfirm}
        title="Xác nhận nạp tiền"
        subtitle="Nhập mật khẩu để xác nhận giao dịch"
        showForgotPassword={false}
        infoContent={
          <p className="text-sm text-gray-600">
            Số tiền nạp: <span className="font-bold text-[#EE501C]">{parseInt(depositAmount || '0').toLocaleString()}đ</span>
          </p>
        }
      />
    </div>
  );
};
