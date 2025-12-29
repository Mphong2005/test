
import React, { useState, useMemo, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { ChevronRight, ChevronDown, CheckCircle2, XCircle, RotateCcw, FileText, Clock, Utensils, X, Bike, Navigation, Star, Store } from 'lucide-react';
import { Order } from '../../types/common';
import { getMyOrdersApi, userCancelOrderApi } from '../../api/orderApi';
import { getFoodByIdApi } from '../../api/productApi';
import { submitReviewApi } from '../../api/reviewApi';
import { useAuthContext } from '../../contexts/AuthContext';
import { formatNumber, extractErrorMessage, filterByField } from '../../utils';
import { ReviewModal } from '../../components/user/ReviewModal';
import { CancelOrderModal } from '../../components/user/CancelOrderModal';
import { ContactShipperModal } from '../../components/user/ContactShipperModal';

export const OrdersPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user } = useAuthContext();

  const initialTab = (location.state as { tab?: string })?.tab || 'all';
  const [activeTab, setActiveTab] = useState(initialTab);
  const [orders, setOrders] = useState<Order[]>([]);
  const [loading, setLoading] = useState(true);

  // Modal States
  const [cancelModalOpen, setCancelModalOpen] = useState(false);
  const [contactModalOpen, setContactModalOpen] = useState(false);
  const [reviewModalOpen, setReviewModalOpen] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);

  const [selectedOrderId, setSelectedOrderId] = useState<string | null>(null);
  const [selectedOrderForReview, setSelectedOrderForReview] = useState<Order | null>(null);

  // Load orders from API
  useEffect(() => {
    const fetchOrders = async () => {
      try {
        setLoading(true);
        const data = await getMyOrdersApi();
        setOrders(data);
      } catch (error) {
        console.error("Failed to fetch orders", error);
      } finally {
        setLoading(false);
      }
    };
    fetchOrders();
  }, [user]);

  // Filters - sử dụng filterByField từ utils cho các filter đơn giản
  const allOrders = orders.filter(o => o.status === 'CANCELLED' || o.isReviewed);
  const pendingOrders = filterByField(orders, 'status', 'PENDING');
  const deliveringOrders = filterByField(orders, 'status', 'DELIVERING');
  const completedOrders = orders.filter(o => o.status === 'COMPLETED' && !o.needsReview && !o.isReviewed);
  const reviewOrders = orders.filter(o => o.status === 'COMPLETED' && o.needsReview && !o.isReviewed);

  const TABS = [
    { id: 'all', label: `Tất cả${allOrders.length > 0 ? ` (${allOrders.length})` : ''}` },
    { id: 'pending', label: `Đang đặt${pendingOrders.length > 0 ? ` (${pendingOrders.length})` : ''}` },
    { id: 'delivering', label: `Đang giao${deliveringOrders.length > 0 ? ` (${deliveringOrders.length})` : ''}` },
    { id: 'delivered', label: `Đã giao${completedOrders.length > 0 ? ` (${completedOrders.length})` : ''}` },
    { id: 'review', label: `Cần đánh giá${reviewOrders.length > 0 ? ` (${reviewOrders.length})` : ''}` },
  ];

  const filteredOrders = useMemo(() => {
    switch (activeTab) {
      case 'pending': return pendingOrders;
      case 'delivering': return deliveringOrders;
      case 'delivered': return completedOrders;
      case 'review': return reviewOrders;
      case 'all':
      default:
        return allOrders;
    }
  }, [activeTab, allOrders, pendingOrders, deliveringOrders, completedOrders, reviewOrders]);

  // Handlers
  const handleCancelClick = (orderId: string) => {
    setSelectedOrderId(orderId);
    setCancelModalOpen(true);
  };

  const handleContactClick = (orderId: string) => {
    setSelectedOrderId(orderId);
    setContactModalOpen(true);
  };

  const getSelectedOrder = () => orders.find(o => o.id === selectedOrderId);

  const getShipperInfo = () => {
    const order = getSelectedOrder();
    if (!order?.shipper) return null;
    return {
      name: order.shipper.fullname || 'Shipper',
      id: order.shipper.shipperId.substring(order.shipper.shipperId.length - 6) || 'N/A',
      phone: order.shipper.phone_number || 'N/A',
      email: order.shipper.email || 'N/A',
    };
  };

  const handleConfirmCancel = async () => {
    if (!selectedOrderId) return;
    setIsProcessing(true);

    try {
      await userCancelOrderApi(selectedOrderId);
      setOrders(prev => prev.map(o => o.id === selectedOrderId ? { ...o, status: 'CANCELLED' } : o));
      setCancelModalOpen(false);
      setSelectedOrderId(null);
    } catch (error) {
      const message = extractErrorMessage(error);
      alert(message);
    } finally {
      setIsProcessing(false);
    }
  };

  const handleConfirmReceived = (orderId: string) => {
    setOrders(prev => prev.map(o => o.id === orderId ? { ...o, status: 'COMPLETED', needsReview: true } : o));
  };

  const handleOpenReviewModal = (order: Order) => {
    setSelectedOrderForReview(order);
    setReviewModalOpen(true);
  };

  const handleSubmitReview = async (rating: number, comment: string) => {
    if (!selectedOrderForReview) return;

    try {
      await submitReviewApi({
        orderId: selectedOrderForReview.id,
        rating,
        comment
      });

      setOrders(prev => prev.map(o =>
        o.id === selectedOrderForReview.id
          ? { ...o, isReviewed: true, needsReview: false }
          : o
      ));

      setReviewModalOpen(false);
      setSelectedOrderForReview(null);
    } catch (error) {
      const message = extractErrorMessage(error);
      if (message.includes('đã được đánh giá')) {
        setOrders(prev => prev.map(o =>
          o.id === selectedOrderForReview.id
            ? { ...o, isReviewed: true, needsReview: false }
            : o
        ));
        setReviewModalOpen(false);
        setSelectedOrderForReview(null);
        alert('Đơn hàng này đã được đánh giá trước đó.');
      } else {
        alert(message);
      }
    }
  };

  const handleViewDetail = (foodId: string) => {
    navigate(`/product/${foodId}`);
  };

  const handleReorder = async (order: Order) => {
    try {
      const food = await getFoodByIdApi(order.foodId);
      if (food) {
        navigate('/checkout', { state: { food, quantity: 1 } });
      } else {
        navigate(`/product/${order.foodId}`);
      }
    } catch (error) {
      console.error('Failed to get food data', error);
      navigate(`/product/${order.foodId}`);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-white flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#EE501C]"></div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto px-4 md:px-8 py-10 animate-in fade-in duration-500 bg-white min-h-screen">
      <nav className="text-xs font-medium text-gray-400 flex items-center gap-2 mb-6 select-none">
        <button onClick={() => navigate('/')} className="hover:text-[#EE501C] transition-colors">Trang chủ</button>
        <ChevronRight className="w-3 h-3" />
        <span className="text-gray-800 font-semibold">Đơn hàng của tôi</span>
      </nav>

      <div className="mb-10">
        <h1 className="text-3xl font-black text-gray-900 mb-2">Đơn hàng của tôi</h1>
        <p className="text-sm text-gray-400">Quản lý và theo dõi trạng thái các đơn hàng của bạn</p>
      </div>

      <div className="border-b border-gray-100 mb-8 overflow-x-auto scrollbar-hide">
        <div className="flex gap-8">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`pb-4 text-sm font-bold whitespace-nowrap transition-all relative ${activeTab === tab.id ? 'text-[#EE501C]' : 'text-gray-500 hover:text-gray-700'}`}
            >
              {tab.label}
              {activeTab === tab.id && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-[#EE501C] rounded-full" />}
            </button>
          ))}
        </div>
      </div>

      <div className="space-y-6">
        {filteredOrders.length > 0 ? (
          filteredOrders.map((order) => (
            <div key={order.id} className="bg-white rounded-3xl overflow-hidden border border-gray-50 shadow-sm flex flex-col hover:shadow-md transition-all">
              <div className="p-6 flex flex-col md:flex-row gap-6">
                <div onClick={() => handleViewDetail(order.foodId)} className="w-full md:w-48 h-36 rounded-2xl overflow-hidden shrink-0 cursor-pointer group">
                  <img src={order.imageUrl} alt={order.restaurantName} className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500" />
                </div>

                <div className="flex-1 flex flex-col justify-between">
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <h3 onClick={() => handleViewDetail(order.foodId)} className="text-lg font-bold text-gray-900 cursor-pointer hover:text-[#EE501C] transition-colors flex items-center gap-2">
                        <Store className="w-4 h-4 text-gray-400" />
                        {order.foodName || order.restaurantName}
                      </h3>

                      {order.isReviewed ? (
                        <div className="bg-green-50 text-green-600 text-[10px] font-bold px-3 py-1 rounded-full border border-green-100 flex items-center gap-1">
                          <CheckCircle2 className="w-3 h-3" /> Hoàn thành
                        </div>
                      ) : order.status === 'COMPLETED' && order.needsReview ? (
                        <div className="bg-yellow-50 text-yellow-600 text-[10px] font-bold px-3 py-1 rounded-full border border-yellow-200 flex items-center gap-1">
                          <Star className="w-3 h-3" /> Cần đánh giá
                        </div>
                      ) : order.status === 'COMPLETED' ? (
                        <div className="bg-green-50 text-green-600 text-[10px] font-bold px-3 py-1 rounded-full border border-green-100 flex items-center gap-1">
                          <CheckCircle2 className="w-3 h-3" /> Giao hàng thành công
                        </div>
                      ) : order.status === 'CANCELLED' ? (
                        <div className="bg-gray-50 text-gray-500 text-[10px] font-bold px-3 py-1 rounded-full border border-gray-100 flex items-center gap-1">
                          <XCircle className="w-3 h-3" /> Đã hủy
                        </div>
                      ) : order.status === 'PENDING' ? (
                        <div className="bg-orange-50 text-orange-600 text-[10px] font-bold px-3 py-1 rounded-full border border-orange-100 flex items-center gap-1">
                          <Utensils className="w-3 h-3" /> Đang đặt
                        </div>
                      ) : (
                        <div className="bg-blue-50 text-blue-600 text-[10px] font-bold px-3 py-1 rounded-full border border-blue-100 flex items-center gap-1">
                          <span className="w-1.5 h-1.5 rounded-full bg-blue-500"></span> Đang giao hàng
                        </div>
                      )}
                    </div>

                    <div className="flex items-center gap-2 text-xs text-gray-400 mb-3">
                      <Clock className="w-3.5 h-3.5" />
                      <span>Đặt lúc {order.orderTime}</span>
                    </div>

                    <div className="bg-gray-50 rounded-xl p-3 mb-4 border border-gray-100">
                      <p className="text-sm text-gray-800 font-medium leading-relaxed">{order.description}</p>
                    </div>
                  </div>

                  <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div className="flex flex-col">
                      <span className="text-[10px] text-gray-400 font-bold uppercase tracking-wider">Tổng thanh toán</span>
                      <span className="text-xl font-black text-[#EE501C]">{formatNumber(order.totalAmount)}đ</span>
                    </div>

                    <div className="flex gap-3">
                      {order.isReviewed ? (
                        <>
                          <button onClick={() => handleViewDetail(order.foodId)} className="flex-1 md:flex-none border border-gray-100 text-gray-500 font-bold py-2.5 px-8 rounded-2xl hover:bg-gray-50 transition-colors text-sm flex items-center justify-center gap-2">
                            <FileText className="w-4 h-4" /> Chi tiết
                          </button>
                          <button onClick={() => handleReorder(order)} className="flex-1 md:flex-none bg-[#EE501C] text-white font-bold py-2.5 px-8 rounded-2xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all text-sm flex items-center justify-center gap-2">
                            <RotateCcw className="w-4 h-4" /> Đặt lại
                          </button>
                        </>
                      ) : order.status === 'PENDING' ? (
                        <>
                          <button onClick={() => handleCancelClick(order.id)} className="flex-1 md:flex-none border border-gray-100 text-gray-500 font-bold py-2.5 px-6 rounded-2xl hover:bg-gray-50 transition-colors text-sm flex items-center justify-center gap-2">
                            <X className="w-4 h-4" /> Huỷ đơn
                          </button>
                          <button onClick={() => handleViewDetail(order.foodId)} className="flex-1 md:flex-none bg-[#EE501C] text-white font-bold py-2.5 px-8 rounded-2xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all text-sm flex items-center justify-center">
                            Chi tiết
                          </button>
                        </>
                      ) : order.status === 'DELIVERING' ? (
                        <>
                          <button onClick={() => handleViewDetail(order.foodId)} className="flex-1 md:flex-none border border-gray-100 text-gray-500 font-bold py-2.5 px-8 rounded-2xl hover:bg-gray-50 transition-colors text-sm flex items-center justify-center">
                            Chi tiết
                          </button>
                          <button className="flex-1 md:flex-none bg-[#EE501C] text-white font-bold py-2.5 px-8 rounded-2xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all text-sm flex items-center justify-center gap-2">
                            <Navigation className="w-4 h-4 fill-white" /> Theo dõi
                          </button>
                        </>
                      ) : order.status === 'COMPLETED' ? (
                        <>
                          <button onClick={() => handleViewDetail(order.foodId)} className="flex-1 md:flex-none border border-gray-100 text-gray-500 font-bold py-2.5 px-8 rounded-2xl hover:bg-gray-50 transition-colors text-sm flex items-center justify-center gap-2">
                            <FileText className="w-4 h-4" /> Chi tiết
                          </button>
                          {order.needsReview ? (
                            <button onClick={() => handleOpenReviewModal(order)} className="flex-1 md:flex-none bg-[#EE501C] text-white font-bold py-2.5 px-8 rounded-2xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all text-sm flex items-center justify-center">
                              Đánh giá
                            </button>
                          ) : (
                            <button onClick={() => handleConfirmReceived(order.id)} className="flex-1 md:flex-none bg-[#EE501C] text-white font-bold py-2.5 px-8 rounded-2xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all text-sm flex items-center justify-center">
                              Đã nhận hàng
                            </button>
                          )}
                        </>
                      ) : (
                        <>
                          <button onClick={() => handleViewDetail(order.foodId)} className="flex-1 md:flex-none border border-gray-100 text-gray-500 font-bold py-2.5 px-8 rounded-2xl hover:bg-gray-50 transition-colors text-sm flex items-center justify-center gap-2">
                            <FileText className="w-4 h-4" /> Chi tiết
                          </button>
                          <button onClick={() => handleReorder(order)} className="flex-1 md:flex-none bg-[#EE501C] text-white font-bold py-2.5 px-8 rounded-2xl shadow-lg shadow-orange-100 hover:bg-[#d44719] transition-all text-sm flex items-center justify-center gap-2">
                            <RotateCcw className="w-4 h-4" /> Đặt lại
                          </button>
                        </>
                      )}
                    </div>
                  </div>
                </div>
              </div>

              {order.status === 'DELIVERING' && order.shipper && (
                <div className="bg-gray-50/50 border-t border-dashed border-gray-100 px-6 py-4 flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="w-8 h-8 rounded-full bg-white shadow-sm flex items-center justify-center text-gray-400">
                      <Bike className="w-4 h-4" />
                    </div>
                    <span className="text-xs font-bold text-gray-700">Dự kiến giao trong 10-15 phút</span>
                  </div>
                  <button onClick={() => handleContactClick(order.id)} className="text-xs font-bold text-[#EE501C] hover:underline transition-all">
                    Liên hệ
                  </button>
                </div>
              )}
            </div>
          ))
        ) : (
          <div className="text-center py-20 bg-gray-50 rounded-[2rem] border border-dashed border-gray-200">
            <p className="text-gray-400 font-medium italic">Không có đơn hàng nào trong mục này.</p>
          </div>
        )}
      </div>

      {filteredOrders.length > 0 && activeTab === 'all' && (
        <div className="mt-12 flex flex-col items-center gap-2">
          <button className="text-gray-400 text-sm font-bold flex items-center gap-2 hover:text-[#EE501C] transition-colors">
            Xem thêm đơn hàng cũ hơn <ChevronDown className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Modals */}
      <ReviewModal
        isOpen={reviewModalOpen}
        onClose={() => { setReviewModalOpen(false); setSelectedOrderForReview(null); }}
        order={selectedOrderForReview}
        onSubmit={handleSubmitReview}
      />

      <CancelOrderModal
        isOpen={cancelModalOpen}
        onClose={() => { setCancelModalOpen(false); setSelectedOrderId(null); }}
        orderName={getSelectedOrder()?.restaurantName || 'cửa hàng này'}
        onConfirm={handleConfirmCancel}
        isProcessing={isProcessing}
      />

      <ContactShipperModal
        isOpen={contactModalOpen}
        onClose={() => { setContactModalOpen(false); setSelectedOrderId(null); }}
        shipper={getShipperInfo()}
      />
    </div>
  );
};
