/**
 * OrderSuccessModal Component
 * Modal hiển thị khi đặt hàng thành công
 */

import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Check } from 'lucide-react';

interface OrderSuccessModalProps {
    isOpen: boolean;
    onViewOrders?: () => void;
    onGoHome?: () => void;
}

export const OrderSuccessModal: React.FC<OrderSuccessModalProps> = ({
    isOpen,
    onViewOrders,
    onGoHome,
}) => {
    const navigate = useNavigate();

    if (!isOpen) return null;

    const handleViewOrders = () => {
        if (onViewOrders) {
            onViewOrders();
        } else {
            navigate('/orders', { state: { tab: 'pending' } });
        }
    };

    const handleGoHome = () => {
        if (onGoHome) {
            onGoHome();
        } else {
            navigate('/');
        }
    };

    return (
        <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/60 backdrop-blur-sm p-4 animate-in fade-in duration-300">
            <div className="bg-white w-full max-w-sm rounded-[2.5rem] overflow-hidden shadow-2xl animate-in zoom-in-95 duration-300">
                <div className="bg-[#EE501C] py-12 px-6 flex items-center justify-center">
                    <div className="w-24 h-24 bg-white/20 rounded-full flex items-center justify-center text-white shadow-inner animate-pulse">
                        <div className="w-16 h-16 bg-white rounded-full flex items-center justify-center text-[#EE501C] shadow-lg">
                            <Check className="w-8 h-8 stroke-[4]" />
                        </div>
                    </div>
                </div>
                <div className="px-8 pb-10 pt-8 text-center">
                    <h2 className="text-2xl font-black text-gray-900 mb-3">Đặt hàng thành công!</h2>
                    <p className="text-sm text-gray-400 leading-relaxed font-medium mb-10 px-2">
                        Cảm ơn bạn đã tin tưởng. Tài xế sẽ sớm liên hệ với bạn để xác nhận đơn hàng.
                    </p>
                    <div className="space-y-4">
                        <button
                            onClick={handleViewOrders}
                            className="w-full bg-[#EE501C] text-white font-black py-4 rounded-full shadow-[0_15px_30px_rgba(238,80,28,0.25)] hover:bg-[#d44719] transition-all transform active:scale-95 text-sm"
                        >
                            Xem đơn hàng
                        </button>
                        <button
                            onClick={handleGoHome}
                            className="w-full bg-white border border-gray-100 text-gray-500 font-bold py-4 rounded-full hover:bg-gray-50 transition-all transform active:scale-95 text-sm shadow-sm"
                        >
                            Trở về trang chủ
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
};
