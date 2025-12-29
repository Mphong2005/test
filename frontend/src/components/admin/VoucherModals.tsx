
import React, { useState, useEffect } from 'react';
import { Modal } from '../common/Modal';
import { Button } from '../common/Button';
import { Voucher } from '../../types/common';
import { DeleteConfirmModal } from '../common/DeleteConfirmModal';

interface VoucherModalsProps {
  modalType: 'ADD' | 'EDIT' | 'DELETE' | null;
  voucher: Voucher | null;
  onClose: () => void;
  onSave: (voucher: Partial<Voucher>) => void;
  onDelete: () => void;
}

export const VoucherModals: React.FC<VoucherModalsProps> = ({
  modalType,
  voucher,
  onClose,
  onSave,
  onDelete
}) => {
  const [formData, setFormData] = useState<Partial<Voucher>>({
    code: '',
    title: '',
    description: '',
    type: 'Percent',
    discountValue: 0,
    maxDiscount: 0,
    minOrderValue: 0,
    startDate: '',
    endDate: '',
    status: 'Active',
    condition: ''
  });

  useEffect(() => {
    if ((modalType === 'EDIT' || modalType === 'ADD') && voucher) {
      setFormData({ ...voucher });
    } else if (modalType === 'ADD') {
      // Reset form for ADD
      setFormData({
        code: '',
        title: '',
        description: '',
        type: 'Percent',
        discountValue: 0,
        maxDiscount: 0,
        minOrderValue: 0,
        startDate: '',
        endDate: '',
        status: 'Active',
        condition: ''
      });
    }
  }, [modalType, voucher]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: name === 'discountValue' || name === 'maxDiscount' || name === 'minOrderValue' ? Number(value) : value
    }));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSave(formData);
  };

  // Helper styles
  const inputClass = "block w-full rounded-lg border border-gray-300 bg-white px-3 py-2.5 text-gray-900 focus:border-[#EE501C] focus:ring-1 focus:ring-[#EE501C] sm:text-sm outline-none transition-shadow";
  const labelClass = "block text-sm font-medium text-gray-700 mb-1";

  return (
    <>
      {/* ADD / EDIT MODAL - Widened to 2xl */}
      <Modal isOpen={modalType === 'ADD' || modalType === 'EDIT'} onClose={onClose} title={modalType === 'ADD' ? 'Thêm Voucher Mới' : 'Cập nhật Voucher'} maxWidth="2xl">
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className={labelClass}>Mã Voucher <span className="text-red-500">*</span></label>
              <input type="text" name="code" required value={formData.code} onChange={handleChange} className={inputClass} placeholder="SUMMER2025" />
            </div>
            <div>
              <label className={labelClass}>Tên Voucher <span className="text-red-500">*</span></label>
              <input type="text" name="title" required value={formData.title} onChange={handleChange} className={inputClass} placeholder="Chào hè rực rỡ" />
            </div>
          </div>

          <div>
            <label className={labelClass}>Mô tả</label>
            <input type="text" name="description" value={formData.description} onChange={handleChange} className={inputClass} placeholder="Giảm giá đồ uống mùa hè" />
          </div>

          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className={labelClass}>Loại</label>
              <select name="type" value={formData.type} onChange={handleChange} className={inputClass}>
                <option value="Percent">Percent</option>
                <option value="Fixed">Fixed</option>
                <option value="FreeShip">FreeShip</option>
              </select>
            </div>
            <div className="col-span-2">
              <label className={labelClass}>Giá trị giảm <span className="text-red-500">*</span></label>
              <input type="number" name="discountValue" required value={formData.discountValue} onChange={handleChange} className={inputClass} placeholder="20" />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className={labelClass}>Giảm tối đa</label>
              <input type="number" name="maxDiscount" value={formData.maxDiscount} onChange={handleChange} className={inputClass} placeholder="50000" />
            </div>
            <div>
              <label className={labelClass}>Đơn tối thiểu</label>
              <input type="number" name="minOrderValue" value={formData.minOrderValue} onChange={handleChange} className={inputClass} placeholder="100000" />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className={labelClass}>Ngày bắt đầu</label>
              <input type="date" name="startDate" value={formData.startDate?.split('T')[0]} onChange={handleChange} className={inputClass} />
            </div>
            <div>
              <label className={labelClass}>Ngày kết thúc</label>
              <input type="date" name="endDate" value={formData.endDate?.split('T')[0]} onChange={handleChange} className={inputClass} />
            </div>
          </div>

          <div>
            <label className={labelClass}>Trạng thái</label>
            <select name="status" value={formData.status} onChange={handleChange} className={inputClass}>
              <option value="Active">Đang hoạt động</option>
              <option value="Inactive">Tạm dừng</option>
              <option value="Expired">Hết hạn</option>
            </select>
          </div>

          <div className="pt-4 border-t border-gray-100 flex justify-end gap-3">
            <Button variant="secondary" type="button" onClick={onClose}>Hủy</Button>
            <Button type="submit">{modalType === 'ADD' ? 'Thêm mới' : 'Lưu thay đổi'}</Button>
          </div>
        </form>
      </Modal>

      {/* DELETE MODAL - sử dụng DeleteConfirmModal component */}
      <DeleteConfirmModal
        isOpen={modalType === 'DELETE'}
        onClose={onClose}
        onConfirm={onDelete}
        title="Xác nhận xóa voucher"
        itemName={voucher?.code || ''}
        description={`Bạn có chắc chắn muốn xóa voucher ${voucher?.code || ''} không? Hành động này không thể hoàn tác.`}
      />
    </>
  );
};
