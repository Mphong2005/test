import React, { useState, useMemo } from 'react';
import { Search, Plus, Edit2, Trash2, Lock, Unlock } from 'lucide-react';
import { useUser } from '../../hooks/useUser';
import { AdminModals } from '../../components/admin/AdminModals';
import { AdminLayout } from '../../layouts/AdminLayout';
import { Status } from '../../types/common';
import { paginate, formatDateVN, getInitials } from '../../utils';
import { Pagination } from '../../components/common/Pagination';

const ITEMS_PER_PAGE = 7;

export const UsersManagement: React.FC = () => {
  // Use custom hook to handle business logic
  const {
    users,
    filteredUsers,
    searchTerm,
    setSearchTerm,
    filterRole,
    setFilterRole,
    modal,
    openModal,
    closeModal,
    handleDeleteUser,
    handleToggleStatus,
    handleSaveUser,
    loading,
    error,
  } = useUser();

  const [currentPage, setCurrentPage] = useState(1);

  // Use pagination utility
  const { data: paginatedUsers, pagination } = useMemo(
    () => paginate(filteredUsers, currentPage, ITEMS_PER_PAGE),
    [filteredUsers, currentPage]
  );

  const { totalPages, startIndex, endIndex, totalItems } = pagination;

  const renderStatusBadge = (status: Status) => {
    const styles = {
      Active: 'bg-green-100 text-green-700 border border-green-200',
      Inactive: 'bg-gray-100 text-gray-700 border border-gray-200',
      Banned: 'bg-red-100 text-red-700 border border-red-200',
    };
    const labels = {
      Active: 'Đang hoạt động',
      Inactive: 'Không hoạt động',
      Banned: 'Bị khóa',
    };
    return (
      <span className={`px-2.5 py-0.5 rounded-full text-xs font-medium ${styles[status]}`}>
        {labels[status]}
      </span>
    );
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#EE501C] mx-auto"></div>
          <p className="mt-4 text-gray-600">Đang tải dữ liệu...</p>
        </div>
      </div>
    );
  }

  return (
    <AdminLayout title="Quản lý Người dùng">
      {/* Content Body */}
      <div className="space-y-6 animate-in slide-in-from-bottom-4 duration-500">

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg">
            {error}
          </div>
        )}

        {/* Header Section */}
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-end gap-4">
          <div>
            <h2 className="text-lg font-medium text-gray-900">Hệ thống quản trị trung tâm</h2>
            <p className="text-sm text-gray-500 mt-1">Xem, chỉnh sửa và quản lý tài khoản users và shippers</p>
          </div>
          <button
            onClick={() => openModal('ADD')}
            className="bg-[#EE501C] hover:bg-[#d64215] text-white px-4 py-2 rounded-lg text-sm font-medium flex items-center shadow-sm transition-colors"
          >
            <Plus className="w-5 h-5 mr-2" />
            Thêm người dùng
          </button>
        </div>

        {/* Filter Bar */}
        <div className="bg-white p-4 rounded-xl border border-gray-200 shadow-sm flex flex-col sm:flex-row gap-4 justify-between items-center">
          <div className="relative w-full sm:flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-5 h-5" />
            <input
              className="block w-full pl-10 pr-3 py-2.5 border border-gray-300 rounded-lg leading-5 bg-white placeholder-gray-400 focus:outline-none focus:ring-1 focus:ring-[#EE501C] focus:border-[#EE501C] sm:text-sm text-gray-900 transition-colors"
              placeholder="Tìm người dùng theo tên, email, hoặc số điện thoại..."
              type="text"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
          <div className="flex gap-3 w-full sm:w-auto">
            <select
              value={filterRole}
              onChange={(e) => setFilterRole(e.target.value)}
              className="block w-full sm:w-48 pl-3 pr-10 py-2.5 text-base border border-gray-300 focus:outline-none focus:ring-[#EE501C] focus:border-[#EE501C] sm:text-sm rounded-lg bg-white text-gray-900 cursor-pointer"
            >
              <option value="All">Tất cả vai trò</option>
              <option value="User">User</option>
              <option value="Shipper">Shipper</option>
            </select>
          </div>
        </div>

        {/* User Table - Inline (giống các page khác) */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">User ID</th>
                  <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Họ và tên</th>
                  <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Email</th>
                  <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Role</th>
                  <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Ngày đăng ký</th>
                  <th scope="col" className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Trạng thái</th>
                  <th scope="col" className="px-6 py-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">Hành động</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {paginatedUsers.length > 0 ? (
                  paginatedUsers.map((user) => (
                    <tr key={user.id} className="hover:bg-gray-50 transition-colors">
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 font-mono">{user.id}</td>
                      <td
                        className="px-6 py-4 whitespace-nowrap cursor-pointer group"
                        onClick={() => openModal('VIEW', user)}
                      >
                        <div className="flex items-center">
                          <div className="h-8 w-8 rounded-full bg-gradient-to-br from-red-100 to-orange-100 flex items-center justify-center text-[#EE501C] font-bold text-xs mr-3 border border-red-200 group-hover:border-[#EE501C] transition-colors">
                            {getInitials(user.name)}
                          </div>
                          <div className="text-sm font-medium text-gray-900 group-hover:text-[#EE501C] transition-colors">{user.name}</div>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600">{user.email}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded text-xs font-medium capitalize
                            ${user.role === 'Admin' ? 'bg-purple-100 text-purple-800' :
                            user.role === 'Shipper' ? 'bg-blue-100 text-blue-800' :
                              'bg-gray-100 text-gray-800'}`}>
                          {user.role}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{formatDateVN(user.joinDate)}</td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        {renderStatusBadge(user.status)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                        <div className="flex items-center justify-end space-x-3">
                          <button
                            onClick={() => openModal('EDIT', user)}
                            className="text-[#EE501C] hover:text-[#d43f0f] transition-colors"
                            title="Chỉnh sửa"
                          >
                            <Edit2 size={18} />
                          </button>
                          <button
                            onClick={() => handleToggleStatus(user)}
                            className={`transition-colors ${user.status === 'Banned' ? 'text-red-500 hover:text-red-600' : 'text-gray-400 hover:text-[#EE501C]'}`}
                            title={user.status === 'Banned' ? "Mở khóa tài khoản" : "Khóa tài khoản"}
                          >
                            {user.status === 'Banned' ? <Unlock size={18} /> : <Lock size={18} />}
                          </button>
                          <button
                            onClick={() => openModal('DELETE', user)}
                            className="text-gray-400 hover:text-red-600 transition-colors"
                            title="Xóa"
                          >
                            <Trash2 size={18} />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={7} className="px-6 py-12 text-center text-gray-500">
                      <div className="flex flex-col items-center justify-center">
                        <div className="bg-gray-100 p-4 rounded-full mb-3">
                          <Search size={24} className="text-gray-400" />
                        </div>
                        <p className="text-lg font-medium text-gray-900">Không tìm thấy kết quả</p>
                        <p className="text-sm mt-1">Thử thay đổi từ khóa tìm kiếm hoặc bộ lọc.</p>
                      </div>
                    </td>
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
            label="người dùng"
          />
        </div>
      </div>

      {/* Admin Modals Component (Handles Add, Edit, View, Delete) */}
      <AdminModals
        modal={modal}
        onClose={closeModal}
        onSave={handleSaveUser}
        onDelete={handleDeleteUser}
        existingUsers={users.map(u => ({ id: u.id, email: u.email, phone: u.phone }))}
      />
    </AdminLayout>
  );
};
