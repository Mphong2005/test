import React from 'react';
import { Modal } from '../common/Modal';
import { Button } from '../common/Button';
import { AlertTriangle } from 'lucide-react';

interface DeleteConfirmModalProps {
    isOpen: boolean;
    onClose: () => void;
    onConfirm: () => void;
    title?: string;
    itemName: string;
    description?: string;
}

export const DeleteConfirmModal: React.FC<DeleteConfirmModalProps> = ({
    isOpen,
    onClose,
    onConfirm,
    title = 'Xóa tài khoản',
    itemName,
    description
}) => {
    return (
        <Modal isOpen={isOpen} onClose={onClose} maxWidth="sm">
            <div className="flex flex-col items-center text-center p-2">
                <div className="h-16 w-16 rounded-full bg-red-100 flex items-center justify-center text-red-600 mb-5">
                    <AlertTriangle size={32} />
                </div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">{title}</h3>
                <p className="text-sm text-gray-500 mb-8 leading-relaxed">
                    {description || (
                        <>
                            Bạn có chắc chắn muốn xóa <strong className="text-gray-900">{itemName}</strong> này không?
                            Tất cả dữ liệu liên quan sẽ bị xóa vĩnh viễn.
                            Hành động này không thể được hoàn tác.
                        </>
                    )}
                </p>

                <div className="flex w-full gap-3">
                    <Button variant="secondary" className="flex-1" onClick={onClose}>
                        Hủy bỏ
                    </Button>
                    <Button variant="danger" className="flex-1" onClick={onConfirm}>
                        Xóa
                    </Button>
                </div>
            </div>
        </Modal>
    );
};
