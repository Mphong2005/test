
import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ShipperOrder, OrderStatus } from '../../types/shipper';
import { getShipperOrdersApi, acceptOrderApi, declineOrderApi } from '../../api/shipperApi';
import { Bell, Store, MapPin, Receipt, ArrowRight, Loader2 } from 'lucide-react';
import { DeclineReasonModal } from '../../components/shipper/DeclineReasonModal';

export const ShipperPendingPage: React.FC = () => {
  const navigate = useNavigate();
  const [orders, setOrders] = useState<ShipperOrder[]>([]);
  const [loading, setLoading] = useState(true);
  const [processingId, setProcessingId] = useState<string | null>(null);

  // Decline Modal State
  const [declineModalOpen, setDeclineModalOpen] = useState(false);
  const [selectedDeclineId, setSelectedDeclineId] = useState<string | null>(null);

  // Load pending orders
  const loadOrders = async () => {
    try {
      setLoading(true);
      const data = await getShipperOrdersApi(OrderStatus.Pending);
      setOrders(data);
    } catch (error) {
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadOrders();
  }, []);

  const handleAccept = async (id: string) => {
    if (processingId) return;
    setProcessingId(id);

    try {
      const success = await acceptOrderApi(id);
      if (success) {
        navigate('/shipper/home');
      } else {
        alert("Không thể nhận đơn hàng này (có thể đã bị hủy hoặc người khác nhận).");
        setProcessingId(null);
        loadOrders();
      }
    } catch (error) {
      console.error(error);
      setProcessingId(null);
    }
  };

  const handleOpenDeclineModal = (id: string) => {
    setSelectedDeclineId(id);
    setDeclineModalOpen(true);
  };

  const handleConfirmDecline = async (reason: string) => {
    if (!selectedDeclineId) return;

    setProcessingId(selectedDeclineId);
    try {
      await declineOrderApi(selectedDeclineId, reason);
      await loadOrders();
    } catch (error) {
      console.error(error);
      alert('Không thể từ chối đơn hàng. Vui lòng thử lại.');
    } finally {
      setProcessingId(null);
      setSelectedDeclineId(null);
    }
  };

  if (loading) {
    return <div className="flex justify-center py-20"><div className="animate-spin rounded-full h-10 w-10 border-b-2 border-[#EE501C]"></div></div>;
  }

  return (
    <div className="flex flex-col gap-8 animate-in fade-in duration-500 pb-20">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h2 className="text-2xl font-black text-gray-900">Đơn hàng mới</h2>
          <p className="text-gray-400 text-sm mt-1 font-medium">Danh sách các đơn hàng đang chờ shipper tiếp nhận</p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 px-5 py-2.5 rounded-full border border-[#EE501C]/20 bg-orange-50 shadow-sm">
            <Bell size={20} className="text-[#EE501C] animate-bounce" />
            <span className="text-sm font-bold text-[#EE501C]">{orders.length} Đơn mới</span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 w-full">
        {orders.map((order) => (
          <div key={order.id} className="bg-white border border-gray-100 rounded-[2.5rem] shadow-sm flex flex-col relative overflow-hidden group hover:shadow-xl transition-all duration-300">
            {/* Header: Store Info & Status */}
            <div className="p-7 flex justify-between items-start bg-gradient-to-b from-gray-50/50 to-white">
              <div className="flex items-center gap-5">
                <div
                  className="w-16 h-16 rounded-2xl bg-cover bg-center shrink-0 border border-gray-100 shadow-sm"
                  style={{ backgroundImage: `url("${order.storeImage}")` }}
                />
                <div>
                  <h3 className="font-bold text-xl text-gray-900 leading-tight mb-1">{order.foodName || order.storeName}</h3>
                  <p className="text-sm text-gray-400 font-medium">Mã đơn: <span className="font-mono text-gray-600">{order.id}</span></p>
                </div>
              </div>
              <span className="px-3 py-1.5 rounded-lg bg-[#FFF9C4] text-[#F9A825] text-[10px] font-black uppercase tracking-wider">
                ĐANG CHUẨN BỊ
              </span>
            </div>

            <div className="px-7">
              <div className="border-t border-dashed border-gray-200"></div>
            </div>

            {/* Address Section */}
            <div className="p-7 flex flex-col gap-6 relative">
              <div className="absolute left-[47px] top-[55px] bottom-[55px] w-px border-l-2 border-dashed border-gray-200"></div>

              <div className="flex gap-5 items-start relative z-10">
                <div className="w-10 h-10 rounded-xl bg-blue-50 flex items-center justify-center shrink-0 text-blue-600">
                  <Store size={20} />
                </div>
                <div className="flex-1 min-w-0 pt-1">
                  <p className="text-[10px] text-gray-400 font-bold uppercase tracking-widest mb-1.5">LẤY HÀNG TẠI</p>
                  <p className="text-sm font-bold text-gray-900 leading-snug">{order.storeAddress}</p>
                </div>
              </div>

              <div className="flex gap-5 items-start relative z-10">
                <div className="w-10 h-10 rounded-xl bg-orange-50 flex items-center justify-center shrink-0 text-[#EE501C]">
                  <MapPin size={20} />
                </div>
                <div className="flex-1 min-w-0 pt-1">
                  <p className="text-[10px] text-gray-400 font-bold uppercase tracking-widest mb-1.5">GIAO ĐẾN</p>
                  <p className="text-sm font-bold text-gray-900 leading-snug">{order.deliveryAddress}</p>
                </div>
              </div>
            </div>

            <div className="px-7">
              <div className="border-t border-dashed border-gray-200"></div>
            </div>

            {/* Order Items */}
            <div className="p-7 flex flex-col gap-4">
              <div className="flex items-center gap-2 mb-1">
                <Receipt size={18} className="text-gray-400" />
                <h4 className="text-sm font-bold text-gray-800">Chi tiết đơn hàng</h4>
              </div>
              <div className="space-y-3">
                {order.items.map((item, idx) => (
                  <div key={idx} className="flex justify-between items-start text-sm">
                    <p className="text-gray-900 font-medium flex gap-2">
                      <span className="text-gray-500 font-bold bg-gray-50 px-2 rounded-md">{item.quantity}x</span>
                      {item.name}
                    </p>
                    <p className="font-bold text-gray-900">{(item.price * item.quantity).toLocaleString('vi-VN')}đ</p>
                  </div>
                ))}
              </div>
            </div>

            <div className="px-7 mt-auto">
              <div className="border-t border-gray-100"></div>
            </div>

            {/* Footer / Actions */}
            <div className="p-7 flex items-center justify-between gap-4 bg-gray-50/30">
              <div className="flex flex-col">
                <p className="text-[10px] text-gray-400 font-bold uppercase tracking-widest mb-1">Thu nhập đơn</p>
                <p className="text-2xl font-black text-[#EE501C] leading-none">{order.totalAmount.toLocaleString('vi-VN')}đ</p>
              </div>

              <div className="flex items-center gap-3">
                <button
                  onClick={() => handleOpenDeclineModal(order.id)}
                  disabled={!!processingId}
                  className="px-6 py-3.5 rounded-2xl border border-gray-200 bg-white text-gray-700 font-bold text-sm hover:bg-gray-50 transition-all disabled:opacity-50"
                >
                  Từ chối
                </button>
                <button
                  onClick={() => handleAccept(order.id)}
                  disabled={!!processingId}
                  className="px-8 py-3.5 rounded-2xl bg-[#EE501C] text-white font-bold text-sm shadow-xl shadow-orange-200 hover:bg-[#d44719] hover:shadow-orange-300 transition-all flex items-center gap-2 active:scale-95 disabled:opacity-80 disabled:cursor-wait"
                >
                  {processingId === order.id ? <Loader2 size={18} className="animate-spin" /> : <ArrowRight size={18} />}
                  <span>Chấp nhận</span>
                </button>
              </div>
            </div>
          </div>
        ))}
        {orders.length === 0 && (
          <div className="col-span-full flex flex-col items-center justify-center py-32 text-center text-gray-400 animate-in fade-in duration-500">
            <div className="w-24 h-24 bg-gray-100 rounded-full flex items-center justify-center mb-6">
              <Bell size={40} className="text-gray-300" />
            </div>
            <h3 className="text-xl font-bold text-gray-900 mb-2">Đang tìm đơn hàng...</h3>
            <p className="text-sm">Hệ thống sẽ tự động thông báo khi có đơn hàng mới gần bạn.</p>
          </div>
        )}
      </div>

      {/* Decline Reason Modal */}
      <DeclineReasonModal
        isOpen={declineModalOpen}
        onClose={() => { setDeclineModalOpen(false); setSelectedDeclineId(null); }}
        onConfirm={handleConfirmDecline}
        isProcessing={!!processingId && processingId === selectedDeclineId}
      />
    </div>
  );
};
