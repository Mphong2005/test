import React, { useMemo } from 'react';
import { AdminLayout } from '../../layouts/AdminLayout';
import {
  Search, Plus, Star, UtensilsCrossed, Lock, Unlock, Trash2
} from 'lucide-react';
import { useRestaurant } from '../../hooks/useRestaurant';
import { paginate, getInitials } from '../../utils';
import { RestaurantModals } from '../../components/admin/RestaurantModals';
import { Restaurant } from '../../types/common';
import { Pagination } from '../../components/common/Pagination';

const ITEMS_PER_PAGE = 7;

export const RestaurantManagement: React.FC = () => {
  // Use hook for restaurant management
  const {
    filteredRestaurants,
    searchTerm,
    setSearchTerm,
    filterStatus,
    setFilterStatus,
    currentPage,
    setCurrentPage,
    loading,
    selectedRestaurant,
    setSelectedRestaurant,
    showDeleteModal,
    setShowDeleteModal,
    showMenuModal,
    setShowMenuModal,
    deleteRestaurant,
    toggleStatus,
  } = useRestaurant();

  // Modal type state
  const activeModal = showMenuModal ? 'MENU' : showDeleteModal ? 'DELETE' : null;

  // Pagination using utils
  const { data: paginatedData, pagination } = useMemo(() => {
    return paginate(filteredRestaurants, currentPage, ITEMS_PER_PAGE);
  }, [filteredRestaurants, currentPage]);

  const { totalItems, totalPages, startIndex, endIndex } = pagination;

  // Modal handlers
  const openMenuModal = (restaurant: Restaurant) => {
    setSelectedRestaurant(restaurant);
    setShowMenuModal(true);
  };

  const openDeleteModal = (restaurant: Restaurant) => {
    setSelectedRestaurant(restaurant);
    setShowDeleteModal(true);
  };

  const closeModals = () => {
    setShowMenuModal(false);
    setShowDeleteModal(false);
    setSelectedRestaurant(null);
  };

  const handleConfirmDelete = async () => {
    if (selectedRestaurant) {
      await deleteRestaurant(selectedRestaurant.id);
    }
  };

  const handleToggleStatus = async (id: string, currentStatus: string) => {
    await toggleStatus(id, currentStatus);
  };

  return (
    <AdminLayout title="Quản lý Nhà hàng">
      <div className="space-y-6 animate-in slide-in-from-bottom-4 duration-500">

        {/* Header Section */}
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-end gap-4">
          <div>
            <h2 className="text-lg font-medium text-gray-900">Danh sách đối tác</h2>
            <p className="text-sm text-gray-500 mt-1">Quản lý thông tin và trạng thái của tất cả nhà hàng trong hệ thống.</p>
          </div>
          <button className="bg-[#EE501C] hover:bg-[#d64215] text-white px-4 py-2 rounded-lg text-sm font-medium flex items-center shadow-sm transition-colors">
            <Plus className="w-5 h-5 mr-2" />
            Thêm nhà hàng mới
          </button>
        </div>

        {/* Filter Bar */}
        <div className="bg-white p-4 rounded-xl border border-gray-200 shadow-sm flex flex-col sm:flex-row gap-4 justify-between items-center">
          <div className="relative w-full sm:flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-5 h-5" />
            <input
              className="block w-full pl-10 pr-3 py-2.5 border border-gray-300 rounded-lg leading-5 bg-white placeholder-gray-400 focus:outline-none focus:ring-1 focus:ring-[#EE501C] focus:border-[#EE501C] sm:text-sm text-gray-900 transition-colors"
              placeholder="Tìm kiếm theo tên nhà hàng, mã ID..."
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
              <option value="Active">Đang hoạt động</option>
              <option value="Inactive">Ngừng hoạt động</option>
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
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Restaurant ID</th>
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Tên nhà hàng</th>
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Địa chỉ</th>
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Liên hệ</th>
                      <th className="px-6 py-4 text-center text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Đánh giá</th>
                      <th className="px-6 py-4 text-left text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Trạng thái</th>
                      <th className="px-6 py-4 text-right text-xs font-bold text-gray-500 uppercase tracking-wider" scope="col">Hoạt động</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {paginatedData.length > 0 ? paginatedData.map((res) => (
                      <tr key={res.id} className="hover:bg-gray-50 transition-colors">
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-semibold text-gray-500 font-mono">{res.id || 'N/A'}</td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex items-center gap-4">
                            <div className={`w-10 h-10 rounded-lg flex items-center justify-center font-bold text-lg shadow-sm flex-shrink-0 ${res.colorClass || 'bg-gray-100 text-gray-600'}`}>
                              {res.initial || getInitials(res.name)}
                            </div>
                            <div>
                              <p className="text-sm font-medium text-gray-900">{res.name}</p>
                              <p className="text-xs text-gray-500">{res.category}</p>
                            </div>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 max-w-xs truncate" title={res.address}>
                          {res.address}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex flex-col">
                            <span className="text-sm text-gray-900 font-medium">{res.phone}</span>
                            <span className="text-xs text-gray-500">{res.email}</span>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-center">
                          <div className="inline-flex items-center gap-1 bg-yellow-50 px-2 py-1 rounded text-yellow-600 font-medium text-xs">
                            <Star className="w-3.5 h-3.5 fill-current" />
                            {res.rating}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          {res.status === 'Active' ? (
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-green-100 text-green-700 border border-green-200">
                              <span className="w-1.5 h-1.5 rounded-full bg-green-500"></span>
                              Đang hoạt động
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-700 border border-gray-200">
                              <span className="w-1.5 h-1.5 rounded-full bg-gray-500"></span>
                              Tạm ngưng
                            </span>
                          )}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="flex items-center justify-end gap-2">
                            <button
                              onClick={() => openMenuModal(res)}
                              className="w-8 h-8 rounded hover:bg-gray-100 text-gray-400 hover:text-[#EE501C] transition-colors flex items-center justify-center"
                              title="Hiển thị menu quán"
                            >
                              <UtensilsCrossed className="w-4 h-4" />
                            </button>
                            <button
                              onClick={() => handleToggleStatus(res.id, res.status)}
                              className="w-8 h-8 rounded hover:bg-gray-100 text-gray-400 hover:text-yellow-500 transition-colors flex items-center justify-center"
                              title={res.status === 'Active' ? "Tạm ngừng" : "Kích hoạt"}
                            >
                              {res.status === 'Active' ? <Lock className="w-4 h-4" /> : <Unlock className="w-4 h-4" />}
                            </button>
                            <button
                              onClick={() => openDeleteModal(res)}
                              className="w-8 h-8 rounded hover:bg-gray-100 text-gray-400 hover:text-red-500 transition-colors flex items-center justify-center"
                              title="Xoá nhà hàng"
                            >
                              <Trash2 className="w-4 h-4" />
                            </button>
                          </div>
                        </td>
                      </tr>
                    )) : (
                      <tr>
                        <td colSpan={7} className="px-6 py-12 text-center text-gray-500 italic">Không tìm thấy nhà hàng nào phù hợp.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              <Pagination
                currentPage={currentPage}
                totalPages={totalPages}
                startIndex={startIndex}
                endIndex={endIndex}
                totalItems={totalItems}
                onPageChange={setCurrentPage}
                label="nhà hàng"
              />
            </>
          )}
        </div>
      </div>

      {/* Render Modals */}
      <RestaurantModals
        modalType={activeModal}
        restaurant={selectedRestaurant}
        onClose={closeModals}
        onConfirmDelete={handleConfirmDelete}
      />
    </AdminLayout>
  );
};