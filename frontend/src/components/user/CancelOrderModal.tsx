/**
 * CancelOrderModal Component
 * Modal xác nhận hủy đơn hàng
 */

import React from 'react';
import { AlertTriangle } from 'lucide-react';
import { Modal } from '../common/Modal';

interface CancelOrderModalProps {
    isOpen: boolean;
    onClose: () => void;
    orderName: string;
    onConfirm: () => Promise<void>;
    isProcessing?: boolean;
}

export const CancelOrderModal: React.FC<CancelOrderModalProps> = ({
    isOpen,
    onClose,
    orderName,
    onConfirm,
    isProcessing = false,
}) => {
    const handleConfirm = async () => {
        await onConfirm();
    };

    return (
        <Modal isOpen={isOpen} onClose={onClose} maxWidth="md" hideCloseButton={false}>
            <div className="flex flex-col items-center text-center p-2">
                <div className="w-20 h-20 rounded-full bg-red-50 flex items-center justify-center text-red-500 mb-5 border border-red-100">
                    <AlertTriangle size={32} />
                </div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">Xác nhận hủy đơn</h3>
                <p className="text-sm text-gray-500 mb-6 px-4">
                    Bạn có chắc muốn hủy đơn tại <strong className="text-gray-900">{orderName}</strong> không?
                </p>
                <div className="w-full bg-[#FFF5F5] border border-red-100 rounded-2xl p-4 mb-8">
                    <p className="text-xs text-red-500 font-medium leading-relaxed">
                        Lưu ý: Hủy đơn hàng nhiều lần có thể ảnh hưởng đến điểm tín nhiệm của bạn.
                    </p>
                </div>
                <div className="flex w-full gap-4">
                    <button
                        onClick={onClose}
                        disabled={isProcessing}
                        className="flex-1 py-3.5 rounded-full border border-red-200 text-red-500 font-bold hover:bg-red-50 transition-all text-sm disabled:opacity-50"
                    >
                        Không hủy
                    </button>
                    <button
                        onClick={handleConfirm}
                        disabled={isProcessing}
                        className="flex-1 py-3.5 rounded-full bg-[#EE501C] text-white font-bold hover:bg-[#d44719] shadow-lg shadow-orange-100 transition-all text-sm disabled:opacity-50"
                    >
                        {isProcessing ? 'Đang xử lý...' : 'Xác nhận hủy'}
                    </button>
                </div>
            </div>
        </Modal>
    );
};
