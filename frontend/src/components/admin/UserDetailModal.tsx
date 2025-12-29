import React from 'react';
import { Modal } from '../common/Modal';
import { Button } from '../common/Button';
import { formatDateVN, getInitials } from '../../utils';

interface UserDetailModalProps {
    isOpen: boolean;
    onClose: () => void;
    user: {
        id: string;
        name: string;
        email: string;
        phone?: string;
        address?: string;
        dob?: string;
        gender?: string;
        joinDate?: string;
        role: string;
    } | null;
}

export const UserDetailModal: React.FC<UserDetailModalProps> = ({ isOpen, onClose, user }) => {
    if (!user) return null;

    return (
        <Modal isOpen={isOpen} onClose={onClose} title="Chi tiết người dùng">
            <div className="space-y-6">
                <div className="flex items-center gap-4">
                    <div className="h-20 w-20 rounded-full bg-red-100 flex items-center justify-center text-[#EE501C] text-3xl font-bold border-4 border-white shadow-sm">
                        {getInitials(user.name)}
                    </div>
                    <div>
                        <h2 className="text-2xl font-bold text-gray-900">{user.name}</h2>
                        <span className="text-sm text-gray-500 capitalize">{user.role}</span>
                    </div>
                </div>

                <div className="space-y-4 pt-2">
                    <div className="grid grid-cols-3 gap-4 border-b border-gray-100 pb-4">
                        <span className="text-sm text-gray-500 col-span-1">User ID</span>
                        <span className="text-sm font-medium text-gray-900 col-span-2 font-mono">{user.id}</span>
                    </div>
                    <div className="grid grid-cols-3 gap-4 border-b border-gray-100 pb-4">
                        <span className="text-sm text-gray-500 col-span-1">Email</span>
                        <span className="text-sm font-medium text-gray-900 col-span-2">{user.email}</span>
                    </div>
                    <div className="grid grid-cols-3 gap-4 border-b border-gray-100 pb-4">
                        <span className="text-sm text-gray-500 col-span-1">Số điện thoại</span>
                        <span className="text-sm font-medium text-gray-900 col-span-2">{user.phone || 'N/A'}</span>
                    </div>
                    <div className="grid grid-cols-3 gap-4 border-b border-gray-100 pb-4">
                        <span className="text-sm text-gray-500 col-span-1">Địa chỉ</span>
                        <span className="text-sm font-medium text-gray-900 col-span-2">{user.address || 'Chưa cập nhật'}</span>
                    </div>
                    <div className="grid grid-cols-3 gap-4 border-b border-gray-100 pb-4">
                        <span className="text-sm text-gray-500 col-span-1">Ngày sinh</span>
                        <span className="text-sm font-medium text-gray-900 col-span-2">
                            {user.dob ? formatDateVN(user.dob) : 'Chưa cập nhật'}
                        </span>
                    </div>
                    <div className="grid grid-cols-3 gap-4 border-b border-gray-100 pb-4">
                        <span className="text-sm text-gray-500 col-span-1">Giới tính</span>
                        <span className="text-sm font-medium text-gray-900 col-span-2">{user.gender || 'Chưa cập nhật'}</span>
                    </div>
                    <div className="grid grid-cols-3 gap-4 border-b border-gray-100 pb-4">
                        <span className="text-sm text-gray-500 col-span-1">Ngày đăng ký</span>
                        <span className="text-sm font-medium text-gray-900 col-span-2">{user.joinDate ? formatDateVN(user.joinDate) : 'N/A'}</span>
                    </div>
                    <div className="grid grid-cols-3 gap-4">
                        <span className="text-sm text-gray-500 col-span-1">Vai trò</span>
                        <span className="text-sm font-medium text-gray-900 col-span-2 capitalize">{user.role}</span>
                    </div>
                </div>

                <div className="flex justify-end pt-4 border-t border-gray-100">
                    <Button variant="secondary" onClick={onClose}>Đóng</Button>
                </div>
            </div>
        </Modal>
    );
};
