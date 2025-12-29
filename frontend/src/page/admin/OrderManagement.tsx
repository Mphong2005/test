import React, { useMemo } from 'react';
import { AdminLayout } from '../../layouts/AdminLayout';
import { Search } from 'lucide-react';
import { useAdminOrder } from '../../hooks/useAdminOrder';
import { paginate, formatNumber, getInitials } from '../../utils';
import { Pagination } from '../../components/common/Pagination';

const ITEMS_PER_PAGE = 7;

export const OrderManagement: React.FC = () => {
  // Use hook for admin order management
  const {
    filteredOrders,
    searchTerm,
    setSearchTerm,
    filterStatus,
    setFilterStatus,
    currentPage,
    setCurrentPage,
    loading,
    getStatusLabel,
    getStatusClasses,
  } = useAdminOrder();

  // Pagination using utils
  const { data: paginatedOrders, pagination } = useMemo(() => {
    return paginate(filteredOrders, currentPage, ITEMS_PER_PAGE);
  }, [filteredOrders, currentPage]);

  const { totalItems, totalPages, startIndex, endIndex } = pagination;

  return (
    <AdminLayout title="Quản lý Đơn hàng">
      <div className="space-y-6 animate-in slide-in-from-bottom-4 duration-500">
        <div>
          <p className="text-sm text-gray-500">Theo dõi, cập nhật và xử lý tất cả các đơn đặt hàng từ hệ thống.</p>
        </div>

        {/* Filter Bar */}
        <div className="bg-white p-4 rounded-xl border border-gray-200 shadow-sm flex flex-col sm:flex-row gap-4 justify-between items-center">
          <div className="relative w-full sm:flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-5 h-5" />
            <input
              className="block w-full pl-10 pr-3 py-2.5 border border-gray-300 rounded-lg leading-5 bg-white placeholder-gray-400 focus:outline-none focus:ring-1 focus:ring-[#EE501C] focus:border-[#EE501C] sm:text-sm text-gray-900 transition-colors"
              placeholder="Tìm đơn hàng theo mã, khách hàng..."
              type="text"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
          <div className="flex gap-3 w-full sm:w-auto">
            <select
              value={filterStatus}
              onChange={(e) => setFilterStatus(e.target.value as any)}
              className="block w-full sm:w-48 pl-3 pr-10 py-2.5 text-base border border-gray-300 focus:outline-none focus:ring-[#EE501C] focus:border-[#EE501C] sm:text-sm rounded-lg bg-white text-gray-900 cursor-pointer"
            >
              <option value="All">Tất cả trạng thái</option>
              <option value="PENDING">Đang chuẩn bị</option>
              <option value="DELIVERING">Đang giao</option>
              <option value="COMPLETED">Hoàn thành</option>
              <option value="CANCELLED">Đã hủy</option>
            </select>
          </div>
        </div>

        {/* Table */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden flex flex-col">
          {loading ? (
            <div className="p-12 text-center text-gray-500">Đang tải dữ liệu...</div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50 border-b border-gray-200">
                    <tr>
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Order ID</th>
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Khách hàng</th>
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Nhà hàng</th>
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Tổng tiền</th>
                      <th className="px-6 py-4 text-center text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Trạng thái</th>
                      <th className="px-6 py-4 text-right text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Thời gian</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {paginatedOrders.length > 0 ? paginatedOrders.map((order) => {
                      const customerName = order.customer || 'Khách lẻ';
                      const restaurantName = order.restaurantName || 'Nhà hàng';
                      const amount = order.totalAmount || 0;
                      const customerEmail = order.email;

                      return (
                        <tr key={order.id} className="hover:bg-gray-50 transition-colors">
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-semibold text-gray-500 font-mono">{order.id}</td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="flex items-center">
                              <div className="h-8 w-8 rounded-full bg-orange-100 text-[#EE501C] flex items-center justify-center text-xs font-bold mr-3 border border-orange-200">
                                {getInitials(customerName)}
                              </div>
                              <div>
                                <div className="text-sm font-medium text-gray-900">{customerName}</div>
                                {customerEmail && <div className="text-[10px] text-gray-400">{customerEmail}</div>}
                              </div>
                            </div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700 font-medium">{restaurantName}</td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-black text-[#EE501C]">{formatNumber(amount)}đ</td>
                          <td className="px-6 py-4 whitespace-nowrap text-center">
                            <span className={`px-2.5 py-1 inline-flex text-[10px] leading-4 font-bold rounded-full uppercase tracking-wide ${getStatusClasses(order.status)}`}>
                              {getStatusLabel(order.status)}
                            </span>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-right text-sm text-gray-500 font-mono">
                            {order.orderTime}
                          </td>
                        </tr>
                      );
                    }) : (
                      <tr>
                        <td colSpan={6} className="px-6 py-12 text-center text-gray-500 italic">
                          Không tìm thấy đơn hàng nào phù hợp.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>

              {/* Pagination Footer */}
              <Pagination
                currentPage={currentPage}
                totalPages={totalPages}
                startIndex={startIndex}
                endIndex={endIndex}
                totalItems={totalItems}
                onPageChange={setCurrentPage}
                label="đơn hàng"
              />
            </>
          )}
        </div>
      </div>
    </AdminLayout>
  );
};