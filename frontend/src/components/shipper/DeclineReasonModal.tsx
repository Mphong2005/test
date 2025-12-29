/**
 * DeclineReasonModal Component
 * Modal cho shipper từ chối đơn hàng với lý do
 */

import React, { useState } from 'react';
import { Modal } from '../common/Modal';
import { AlertTriangle } from 'lucide-react';

const DECLINE_REASONS = [
    'Khoảng cách quá xa',
    'Đang bận việc cá nhân',
    'Sự cố phương tiện',
];

interface DeclineReasonModalProps {
    isOpen: boolean;
    onClose: () => void;
    onConfirm: (reason: string) => Promise<void>;
    isProcessing?: boolean;
}

export const DeclineReasonModal: React.FC<DeclineReasonModalProps> = ({
    isOpen,
    onClose,
    onConfirm,
    isProcessing = false,
}) => {
    const [selectedReason, setSelectedReason] = useState('');
    const [customReason, setCustomReason] = useState('');

    const handleClose = () => {
        setSelectedReason('');
        setCustomReason('');
        onClose();
    };

    const handleConfirm = async () => {
        if (!selectedReason) return;
        if (selectedReason === 'Khác' && !customReason.trim()) return;

        const finalReason = selectedReason === 'Khác' ? customReason : selectedReason;
        await onConfirm(finalReason);
        handleClose();
    };

    const isValid = selectedReason && (selectedReason !== 'Khác' || customReason.trim());

    return (
        <Modal isOpen={isOpen} onClose={handleClose} maxWidth="md">
            <div className="flex flex-col">
                {/* Header */}
                <div className="flex items-center gap-3 mb-6">
                    <div className="w-10 h-10 rounded-full bg-red-50 flex items-center justify-center text-[#EE501C] border border-orange-100">
                        <AlertTriangle size={20} />
                    </div>
                    <h3 className="text-lg font-black text-gray-900">Lý do từ chối đơn hàng</h3>
                </div>

                {/* Warning */}
                <div className="bg-orange-50 border border-orange-100 rounded-2xl p-4 mb-6">
                    <p className="text-sm text-orange-800 leading-relaxed font-medium">
                        Việc từ chối đơn hàng quá nhiều có thể ảnh hưởng đến hiệu suất làm việc và đánh giá của bạn.
                    </p>
                </div>

                <p className="text-sm font-bold text-gray-700 mb-4">Vui lòng chọn lý do hoặc nhập lý do khác:</p>

                {/* Options */}
                <div className="space-y-3 mb-4">
                    {DECLINE_REASONS.map((reason) => (
                        <label key={reason} className="flex items-center gap-3 p-3.5 border border-gray-100 rounded-2xl cursor-pointer hover:bg-gray-50 hover:border-gray-300 transition-all select-none">
                            <div className="relative flex items-center">
                                <input
                                    type="radio"
                                    name="declineReason"
                                    value={reason}
                                    checked={selectedReason === reason}
                                    onChange={(e) => setSelectedReason(e.target.value)}
                                    className="peer h-5 w-5 cursor-pointer appearance-none rounded-full border border-gray-300 transition-all checked:border-[#EE501C] checked:bg-[#EE501C]"
                                />
                                <div className="pointer-events-none absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 text-white opacity-0 peer-checked:opacity-100">
                                    <svg width="10" height="8" viewBox="0 0 12 10" fill="none" xmlns="http://www.w3.org/2000/svg">
                                        <path d="M1 5L4.5 8.5L11 1" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                                    </svg>
                                </div>
                            </div>
                            <span className="text-sm font-medium text-gray-700">{reason}</span>
                        </label>
                    ))}

                    <label className="flex items-center gap-3 p-3.5 border border-gray-100 rounded-2xl cursor-pointer hover:bg-gray-50 hover:border-gray-300 transition-all select-none">
                        <div className="relative flex items-center">
                            <input
                                type="radio"
                                name="declineReason"
                                value="Khác"
                                checked={selectedReason === 'Khác'}
                                onChange={(e) => setSelectedReason(e.target.value)}
                                className="peer h-5 w-5 cursor-pointer appearance-none rounded-full border border-gray-300 transition-all checked:border-[#EE501C] checked:bg-[#EE501C]"
                            />
                            <div className="pointer-events-none absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 text-white opacity-0 peer-checked:opacity-100">
                                <svg width="10" height="8" viewBox="0 0 12 10" fill="none" xmlns="http://www.w3.org/2000/svg">
                                    <path d="M1 5L4.5 8.5L11 1" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                                </svg>
                            </div>
                        </div>
                        <span className="text-sm font-medium text-gray-700">Khác</span>
                    </label>
                </div>

                {/* Textarea */}
                {selectedReason === 'Khác' && (
                    <div className="animate-in fade-in slide-in-from-top-2 duration-200">
                        <textarea
                            className="w-full border border-gray-200 bg-white rounded-2xl p-4 text-sm focus:outline-none focus:ring-2 focus:ring-orange-100 focus:border-[#EE501C] transition-all resize-none mb-6 font-medium text-gray-900 placeholder:text-gray-400"
                            placeholder="Nhập lý do cụ thể..."
                            rows={3}
                            value={customReason}
                            onChange={(e) => setCustomReason(e.target.value)}
                            autoFocus
                        />
                    </div>
                )}

                {/* Footer */}
                <div className="flex gap-3 pt-4 border-t border-gray-100 mt-2">
                    <button
                        onClick={handleClose}
                        className="flex-1 py-3.5 rounded-full border border-gray-200 font-bold text-gray-600 hover:bg-gray-50 hover:text-gray-900 transition-all text-sm"
                    >
                        Hủy
                    </button>
                    <button
                        onClick={handleConfirm}
                        disabled={!isValid || isProcessing}
                        className="flex-1 py-3.5 rounded-full bg-[#EE501C] text-white font-bold hover:bg-[#d44719] transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-orange-200 text-sm"
                    >
                        {isProcessing ? 'Đang xử lý...' : 'Xác nhận từ chối'}
                    </button>
                </div>
            </div>
        </Modal>
    );
};
