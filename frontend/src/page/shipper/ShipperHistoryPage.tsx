
import React, { useState, useRef, useEffect, useMemo } from 'react';
import { ShipperOrder, OrderStatus } from '../../types/shipper';
import { getShipperOrdersApi } from '../../api/shipperApi';
import { Calendar, ChevronDown, Filter, Clock, History } from 'lucide-react';
import { groupByDate, parseOrderDate, filterByField } from '../../utils';
import { ShipperOrderCard } from '../../components/shipper/ShipperOrderCard';

export const ShipperHistoryPage: React.FC = () => {
  const [orders, setOrders] = useState<ShipperOrder[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedMonth, setSelectedMonth] = useState<number>(new Date().getMonth() + 1);
  const [selectedStatus, setSelectedStatus] = useState<'All' | OrderStatus.Completed | OrderStatus.Cancelled>('All');

  const [isMonthOpen, setIsMonthOpen] = useState(false);
  const [isStatusOpen, setIsStatusOpen] = useState(false);

  const monthRef = useRef<HTMLDivElement>(null);
  const statusRef = useRef<HTMLDivElement>(null);

  // Fetch History Orders
  useEffect(() => {
    const fetchOrders = async () => {
      try {
        setLoading(true);
        const data = await getShipperOrdersApi('HISTORY');
        setOrders(data);
      } catch (error) {
        console.error(error);
      } finally {
        setLoading(false);
      }
    };
    fetchOrders();
  }, []);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (monthRef.current && !monthRef.current.contains(event.target as Node)) setIsMonthOpen(false);
      if (statusRef.current && !statusRef.current.contains(event.target as Node)) setIsStatusOpen(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Filter orders using utility functions
  const filteredOrders = useMemo(() => {
    // Filter by status using filterByField utility
    const statusFiltered = filterByField(orders, 'status', selectedStatus);

    // Filter by month
    return statusFiltered.filter(order => {
      const orderDate = parseOrderDate(order.time);
      if (!orderDate) return true; // Include if can't parse date
      return (orderDate.getMonth() + 1) === selectedMonth;
    });
  }, [orders, selectedStatus, selectedMonth]);

  // Group orders by date using utility
  const groupedOrders = useMemo(() => {
    return groupByDate(filteredOrders, (order) => order.time);
  }, [filteredOrders]);

  // Total Earnings from filtered orders
  const totalEarnings = filteredOrders.reduce((acc, curr) =>
    acc + (curr.status === OrderStatus.Completed ? curr.totalAmount : 0), 0
  );

  const months = Array.from({ length: 12 }, (_, i) => i + 1);
  const statuses = [
    { label: 'Tất cả', value: 'All' },
    { label: 'Hoàn thành', value: OrderStatus.Completed },
    { label: 'Đã hủy', value: OrderStatus.Cancelled },
  ];

  if (loading) {
    return <div className="flex justify-center py-20"><div className="animate-spin rounded-full h-10 w-10 border-b-2 border-[#EE501C]"></div></div>;
  }

  return (
    <div className="flex flex-col gap-8 animate-in fade-in duration-500 pb-20">
      {/* Header Section */}
      <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-6">
        <div>
          <h2 className="text-2xl font-black text-gray-900">Lịch sử hoạt động</h2>
          <p className="text-gray-400 text-sm mt-1 font-medium">Xem lại danh sách các đơn hàng đã hoàn thành</p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          {/* Month Selector */}
          <div className="relative" ref={monthRef}>
            <button
              onClick={() => setIsMonthOpen(!isMonthOpen)}
              className="flex items-center gap-2 px-5 py-2.5 rounded-full border border-gray-200 bg-white hover:bg-gray-50 transition-all shadow-sm text-sm font-bold text-gray-700 min-w-[150px] justify-between"
            >
              <div className="flex items-center gap-2">
                <Calendar size={18} className="text-[#EE501C]" />
                Tháng {selectedMonth}
              </div>
              <ChevronDown size={16} className={`text-gray-400 transition-transform ${isMonthOpen ? 'rotate-180' : ''}`} />
            </button>
            {isMonthOpen && (
              <div className="absolute top-full mt-2 left-0 w-full bg-white border border-gray-200 rounded-2xl shadow-xl z-50 max-h-60 overflow-y-auto py-2">
                {months.map(m => (
                  <button
                    key={m}
                    onClick={() => { setSelectedMonth(m); setIsMonthOpen(false); }}
                    className={`w-full text-left px-5 py-2 hover:bg-gray-50 text-sm font-medium ${selectedMonth === m ? 'text-[#EE501C] bg-orange-50' : 'text-gray-900'}`}
                  >
                    Tháng {m}
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Status Selector */}
          <div className="relative" ref={statusRef}>
            <button
              onClick={() => setIsStatusOpen(!isStatusOpen)}
              className="flex items-center gap-2 px-5 py-2.5 rounded-full border border-gray-200 bg-white hover:bg-gray-50 transition-all shadow-sm text-sm font-bold text-gray-700 min-w-[200px] justify-between"
            >
              <div className="flex items-center gap-2">
                <Filter size={18} className="text-[#EE501C]" />
                Trạng thái: {statuses.find(s => s.value === selectedStatus)?.label}
              </div>
              <ChevronDown size={16} className={`text-gray-400 transition-transform ${isStatusOpen ? 'rotate-180' : ''}`} />
            </button>
            {isStatusOpen && (
              <div className="absolute top-full mt-2 right-0 w-full bg-white border border-gray-200 rounded-2xl shadow-xl z-50 py-2">
                {statuses.map(s => (
                  <button
                    key={s.value}
                    onClick={() => { setSelectedStatus(s.value as any); setIsStatusOpen(false); }}
                    className={`w-full text-left px-5 py-2 hover:bg-gray-50 text-sm font-medium ${selectedStatus === s.value ? 'text-[#EE501C] bg-orange-50' : 'text-gray-900'}`}
                  >
                    {s.label}
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Stats Summary Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="p-6 rounded-[2rem] bg-white border border-gray-100 shadow-sm flex flex-col gap-1 hover:shadow-md transition-all">
          <p className="text-[11px] text-gray-400 font-bold uppercase tracking-tight">Tổng đơn tháng này</p>
          <p className="text-3xl font-black text-gray-900">{filteredOrders.length}</p>
        </div>
        <div className="p-6 rounded-[2rem] bg-white border border-gray-100 shadow-sm flex flex-col gap-1 hover:shadow-md transition-all">
          <p className="text-[11px] text-gray-400 font-bold uppercase tracking-tight">Thu nhập ước tính</p>
          <p className="text-3xl font-black text-[#EE501C]">{totalEarnings.toLocaleString('vi-VN')}đ</p>
        </div>
        <div className="p-6 rounded-[2rem] bg-white border border-gray-100 shadow-sm flex flex-col gap-1 hover:shadow-md transition-all">
          <p className="text-[11px] text-gray-400 font-bold uppercase tracking-tight">Tổng thời gian chạy</p>
          <div className="flex items-center gap-2">
            <p className="text-3xl font-black text-gray-900">23h 15p</p>
            <Clock size={20} className="text-green-500 fill-green-100" />
          </div>
        </div>
        <div className="p-6 rounded-[2rem] bg-white border border-gray-100 shadow-sm flex flex-col gap-1 hover:shadow-md transition-all">
          <p className="text-[11px] text-gray-400 font-bold uppercase tracking-tight">Tổng quãng đường</p>
          <p className="text-3xl font-black text-gray-900">34km</p>
        </div>
      </div>

      {/* Date Groups and List */}
      <div className="flex flex-col gap-10">
        {groupedOrders.length > 0 ? (
          groupedOrders.map((group) => (
            <div key={group.key} className="flex flex-col gap-5">
              <div className="flex items-center gap-4">
                {group.label && (
                  <h3 className="text-lg font-bold text-gray-900 whitespace-nowrap min-w-[80px]">{group.label}</h3>
                )}
                <div className="h-px flex-1 bg-gray-200"></div>
                {group.displayDate && (
                  <span className="text-xs text-gray-400 font-bold uppercase tracking-wider">{group.displayDate}</span>
                )}
              </div>

              <div className="grid grid-cols-1 gap-4">
                {group.items.map((order) => (
                  <ShipperOrderCard key={order.id} order={order} />
                ))}
              </div>
            </div>
          ))
        ) : (
          <div className="py-20 text-center text-gray-400">
            <div className="w-20 h-20 rounded-full bg-gray-100 flex items-center justify-center mx-auto mb-6">
              <History size={40} className="text-gray-300" />
            </div>
            <p className="font-bold text-gray-900 mb-1">Không tìm thấy dữ liệu</p>
            <p className="text-sm">Không tìm thấy đơn hàng nào phù hợp với bộ lọc hiện tại.</p>
          </div>
        )}
      </div>
    </div>
  );
};
