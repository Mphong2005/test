
import React, { useMemo } from 'react';
import { AdminLayout } from '../../layouts/AdminLayout';
import {
  Search, Plus, Edit2, Trash2, Calendar, CalendarOff, AlertCircle
} from 'lucide-react';
import { useVoucher } from '../../hooks/useVoucher';
import { paginate, formatCurrency, formatDateVN } from '../../utils';
import { Voucher } from '../../types/common';
import { Pagination } from '../../components/common/Pagination';
import { VoucherModals } from '../../components/admin/VoucherModals';

const ITEMS_PER_PAGE = 7;

export const VoucherManagement: React.FC = () => {
  // Use hook for voucher management
  const {
    filteredVouchers,
    searchTerm,
    setSearchTerm,
    filterStatus,
    setFilterStatus,
    currentPage,
    setCurrentPage,
    loading,
    selectedVoucher,
    setSelectedVoucher,
    showDeleteModal,
    setShowDeleteModal,
    showEditModal,
    setShowEditModal,
    loadVouchers,
    createVoucher,
    updateVoucher,
    deleteVoucher,
    toggleStatus,
  } = useVoucher();

  // Modal type state (ADD, EDIT, DELETE)
  const activeModal = showEditModal ? (selectedVoucher ? 'EDIT' : 'ADD') : showDeleteModal ? 'DELETE' : null;

  // Pagination using utils
  const { data: paginatedData, pagination } = useMemo(() => {
    return paginate(filteredVouchers, currentPage, ITEMS_PER_PAGE);
  }, [filteredVouchers, currentPage]);

  const { totalItems, totalPages, startIndex, endIndex } = pagination;

  // Handlers
  const handleToggleStatus = async (voucher: Voucher) => {
    if (voucher.status === 'Expired') return;
    await toggleStatus(voucher);
  };

  const handleSave = async (voucherData: Partial<Voucher>) => {
    if (activeModal === 'ADD') {
      await createVoucher(voucherData);
    } else if (activeModal === 'EDIT' && selectedVoucher) {
      await updateVoucher(selectedVoucher.id, voucherData);
    }
    setShowEditModal(false);
    setSelectedVoucher(null);
  };

  const handleDelete = async () => {
    if (selectedVoucher) {
      await deleteVoucher(selectedVoucher.id);
    }
  };

  const openAddModal = () => {
    setSelectedVoucher(null);
    setShowEditModal(true);
  };

  const openEditModal = (voucher: Voucher) => {
    setSelectedVoucher(voucher);
    setShowEditModal(true);
  };

  const openDeleteModal = (voucher: Voucher) => {
    setSelectedVoucher(voucher);
    setShowDeleteModal(true);
  };

  const closeModal = () => {
    setShowEditModal(false);
    setShowDeleteModal(false);
    setSelectedVoucher(null);
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'Active': return <span className="px-3 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-green-100 text-green-800">Đang hoạt động</span>;
      case 'Inactive': return <span className="px-3 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-yellow-100 text-yellow-800">Tạm dừng</span>;
      case 'Expired': return <span className="px-3 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-gray-100 text-gray-800">Hết hạn</span>;
      default: return null;
    }
  };

  const getTypeBadge = (type: string) => {
    switch (type) {
      case 'Percent': return <span className="text-xs font-medium bg-blue-50 text-blue-700 px-2 py-1 rounded-md">Percent</span>;
      case 'Fixed': return <span className="text-xs font-medium bg-purple-50 text-purple-700 px-2 py-1 rounded-md">Fixed</span>;
      case 'FreeShip': return <span className="text-xs font-medium bg-orange-50 text-orange-700 px-2 py-1 rounded-md">FreeShip</span>;
      default: return <span className="text-xs font-medium bg-gray-50 text-gray-700 px-2 py-1 rounded-md">{type}</span>;
    }
  };

  return (
    <AdminLayout title="Quản lý Voucher">
      <div className="space-y-6 animate-in slide-in-from-bottom-4 duration-500">

        {/* Header Actions */}
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
          <p className="text-sm text-gray-500">
            Quản lý, tạo mới và theo dõi các mã giảm giá trong hệ thống.
          </p>
          <button
            onClick={openAddModal}
            className="bg-[#EE501C] hover:bg-[#d64215] text-white px-4 py-2 rounded-lg text-sm font-medium shadow-sm transition-colors flex items-center"
          >
            <Plus size={18} className="mr-2" />
            Thêm Voucher
          </button>
        </div>

        {/* Filters */}
        <div className="bg-white p-4 rounded-xl border border-gray-200 shadow-sm flex flex-col sm:flex-row gap-4 justify-between items-center">
          <div className="relative w-full sm:w-96">
            <span className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Search className="text-gray-400" size={20} />
            </span>
            <input
              className="block w-full pl-10 pr-3 py-2.5 border border-gray-300 rounded-lg leading-5 bg-white placeholder-gray-500 focus:outline-none focus:ring-1 focus:ring-[#EE501C] focus:border-[#EE501C] sm:text-sm text-gray-900 transition-colors"
              placeholder="Tìm voucher theo mã, tên..."
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
              <option value="Inactive">Tạm dừng</option>
              <option value="Expired">Hết hạn</option>
            </select>
          </div>
        </div>

        {/* Table */}
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
          {loading ? (
            <div className="p-12 text-center text-gray-500">Đang tải dữ liệu...</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Mã Voucher</th>
                    <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Tên Voucher</th>
                    <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Loại Voucher</th>
                    <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Giá trị giảm</th>
                    <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Giảm tối đa</th>
                    <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Giá trị tối thiểu</th>
                    <th scope="col" className="px-6 py-4 text-center text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Trạng thái</th>
                    <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Thời gian áp dụng</th>
                    <th scope="col" className="px-6 py-4 text-center text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap">Hành động</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {paginatedData.length > 0 ? paginatedData.map((v) => (
                    <tr key={v.id} className={`hover:bg-gray-50 transition-colors ${v.status === 'Expired' ? 'opacity-75' : ''}`}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-bold text-[#EE501C]">{v.code}</td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-sm font-medium text-gray-900">{v.title}</div>
                        <div className="text-xs text-gray-500">{v.description}</div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        {getTypeBadge(v.type)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-semibold text-gray-900">
                        {v.type === 'Percent' ? `${v.discountValue}%` : formatCurrency(v.discountValue)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {v.maxDiscount ? formatCurrency(v.maxDiscount) : '-'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {formatCurrency(v.minOrderValue)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-center">
                        {getStatusBadge(v.status)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-xs text-gray-500">
                          <div className="flex items-center mb-1"><Calendar size={14} className="mr-1" /> {formatDateVN(v.startDate)}</div>
                          <div className="flex items-center"><CalendarOff size={14} className="mr-1" /> {formatDateVN(v.endDate)}</div>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-center">
                        <div className="flex items-center justify-center space-x-2">
                          <button
                            onClick={() => openEditModal(v)}
                            className="text-blue-500 hover:text-blue-700 transition-colors p-1"
                            title="Chỉnh sửa"
                          >
                            <Edit2 size={20} />
                          </button>

                          {/* Custom Toggle Switch using Tailwind */}
                          <label className={`relative inline-flex items-center cursor-pointer ${v.status === 'Expired' ? 'cursor-not-allowed opacity-50' : ''}`}>
                            <input
                              type="checkbox"
                              className="sr-only peer"
                              checked={v.status === 'Active'}
                              disabled={v.status === 'Expired'}
                              onChange={() => handleToggleStatus(v)}
                            />
                            <div className="w-8 h-4 bg-gray-300 peer-focus:outline-none peer-focus:ring-0 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[0px] after:left-[0px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-[#EE501C]"></div>
                          </label>

                          <button
                            onClick={() => openDeleteModal(v)}
                            className="text-red-500 hover:text-red-700 transition-colors p-1"
                            title="Xoá"
                          >
                            <Trash2 size={20} />
                          </button>
                        </div>
                      </td>
                    </tr>
                  )) : (
                    <tr>
                      <td colSpan={9} className="px-6 py-12 text-center text-gray-500">
                        <div className="flex flex-col items-center justify-center">
                          <AlertCircle size={24} className="mb-2 text-gray-300" />
                          <p>Không tìm thấy voucher nào</p>
                        </div>
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}

          <Pagination
            currentPage={currentPage}
            totalPages={totalPages}
            startIndex={startIndex}
            endIndex={endIndex}
            totalItems={totalItems}
            onPageChange={setCurrentPage}
            label="voucher"
          />
        </div>
      </div>

      <VoucherModals
        modalType={activeModal}
        voucher={selectedVoucher}
        onClose={closeModal}
        onSave={handleSave}
        onDelete={handleDelete}
      />
    </AdminLayout>
  );
};
