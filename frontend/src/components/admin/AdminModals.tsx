import React from 'react';
import { AdminModalsProps } from '../../types/admin';
import { UserDetailModal } from './UserDetailModal';
import { UserFormModal } from './UserFormModal';
import { DeleteConfirmModal } from '../common/DeleteConfirmModal';

export const AdminModals: React.FC<AdminModalsProps> = ({ modal, onClose, onSave, onDelete, existingUsers = [] }) => {
  return (
    <>
      {/* VIEW USER DETAILS MODAL */}
      <UserDetailModal
        isOpen={modal.type === 'VIEW'}
        onClose={onClose}
        user={modal.data}
      />

      {/* ADD / EDIT USER MODAL */}
      <UserFormModal
        isOpen={modal.type === 'ADD' || modal.type === 'EDIT'}
        onClose={onClose}
        onSave={onSave}
        mode={modal.type === 'ADD' ? 'ADD' : 'EDIT'}
        initialData={modal.data}
        existingUsers={existingUsers}
      />

      {/* DELETE CONFIRMATION MODAL */}
      <DeleteConfirmModal
        isOpen={modal.type === 'DELETE'}
        onClose={onClose}
        onConfirm={onDelete}
        title="Xóa tài khoản"
        itemName={modal.data?.name || ''}
        description={`Bạn có chắc chắn muốn xóa tài khoản ${modal.data?.name || ''} này không? Tất cả dữ liệu liên quan đến tài khoản này sẽ bị xóa vĩnh viễn. Hành động này không thể được hoàn tác.`}
      />
    </>
  );
};
