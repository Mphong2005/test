/**
 * ContactShipperModal Component
 * Modal hiển thị thông tin shipper và cho phép liên hệ
 */

import React from 'react';
import { X, Phone, Mail, Copy, User, Check } from 'lucide-react';
import { Modal } from '../common/Modal';
import { getInitials } from '../../utils';
import { ShipperInfo } from '../../types/shipper';

interface ContactShipperModalProps {
    isOpen: boolean;
    onClose: () => void;
    shipper: ShipperInfo | null;
}

export const ContactShipperModal: React.FC<ContactShipperModalProps> = ({
    isOpen,
    onClose,
    shipper,
}) => {
    const handleCopy = (text: string) => {
        navigator.clipboard.writeText(text);
    };

    const handleCall = () => {
        if (shipper?.phone) {
            window.location.href = `tel:${shipper.phone.replace(/\s/g, '')}`;
        }
    };

    return (
        <Modal isOpen={isOpen} onClose={onClose} maxWidth="md" hideCloseButton={true}>
            <div className="flex flex-col -m-6 bg-white relative rounded-2xl overflow-hidden">
                {/* Orange Header */}
                <div className="bg-[#EE501C] h-32 px-6 pt-6 relative">
                    <div className="absolute -top-10 -right-10 w-40 h-40 bg-white/10 rounded-full"></div>

                    <div className="relative z-10 flex justify-between items-start">
                        <div className="text-white">
                            <div className="flex items-center gap-2 mb-1">
                                <User size={20} className="fill-white" />
                                <h3 className="font-bold text-lg">Liên hệ Shipper</h3>
                            </div>
                            <p className="text-white/90 text-xs font-medium">Tài xế đang giao đơn hàng của bạn</p>
                        </div>
                        <button
                            onClick={onClose}
                            className="bg-white/20 hover:bg-white/30 text-white p-2 rounded-full transition-all backdrop-blur-sm"
                        >
                            <X size={20} />
                        </button>
                    </div>
                </div>

                {/* Body Content */}
                <div className="px-6 pb-8 pt-14 relative">
                    {!shipper ? (
                        <div className="text-center py-8">
                            <p className="text-gray-500 font-medium">Chưa có thông tin shipper</p>
                            <button
                                onClick={onClose}
                                className="mt-4 px-8 py-3 rounded-2xl bg-[#EE501C] text-white font-bold hover:bg-[#d44719] transition-all"
                            >
                                Đóng
                            </button>
                        </div>
                    ) : (
                        <>
                            {/* Floating Avatar */}
                            <div className="absolute -top-12 left-1/2 -translate-x-1/2">
                                <div className="relative">
                                    <div className="w-24 h-24 rounded-full border-4 border-white shadow-md overflow-hidden bg-gradient-to-br from-[#EE501C] to-[#FF7043] flex items-center justify-center text-white text-3xl font-black">
                                        {getInitials(shipper.name)}
                                    </div>
                                    {/* Verified Badge */}
                                    <div className="absolute bottom-0 right-0 bg-[#00C853] text-white p-1 rounded-full border-[3px] border-white flex items-center justify-center">
                                        <Check size={12} strokeWidth={4} />
                                    </div>
                                </div>
                            </div>

                            {/* Name & ID */}
                            <div className="text-center mb-8">
                                <h4 className="text-xl font-bold text-gray-900 mb-2">{shipper.name}</h4>
                                <span className="inline-block bg-gray-100 text-gray-600 text-xs font-bold px-4 py-1.5 rounded-full">
                                    Mã tài xế: {shipper.id}
                                </span>
                            </div>

                            {/* Contact Cards */}
                            <div className="space-y-4 mb-8">
                                {/* Phone */}
                                <div className="bg-white border border-gray-100 rounded-2xl p-4 flex items-center shadow-sm">
                                    <div className="w-12 h-12 rounded-2xl bg-[#FFF5F1] flex items-center justify-center text-[#EE501C] mr-4 shrink-0">
                                        <Phone size={24} />
                                    </div>
                                    <div className="flex-1">
                                        <p className="text-[10px] text-gray-400 font-bold uppercase mb-0.5">SỐ ĐIỆN THOẠI</p>
                                        <p className="text-base font-bold text-gray-900">{shipper.phone}</p>
                                    </div>
                                    <button
                                        onClick={() => handleCopy(shipper.phone)}
                                        className="text-gray-400 hover:text-[#EE501C] p-2"
                                    >
                                        <Copy size={20} />
                                    </button>
                                </div>

                                {/* Email */}
                                <div className="bg-white border border-gray-100 rounded-2xl p-4 flex items-center shadow-sm">
                                    <div className="w-12 h-12 rounded-2xl bg-[#FFF5F1] flex items-center justify-center text-[#EE501C] mr-4 shrink-0">
                                        <Mail size={24} />
                                    </div>
                                    <div className="flex-1 min-w-0">
                                        <p className="text-[10px] text-gray-400 font-bold uppercase mb-0.5">EMAIL</p>
                                        <p className="text-base font-bold text-gray-900 truncate">{shipper.email}</p>
                                    </div>
                                    <button
                                        onClick={() => handleCopy(shipper.email)}
                                        className="text-gray-400 hover:text-[#EE501C] p-2"
                                    >
                                        <Copy size={20} />
                                    </button>
                                </div>
                            </div>

                            {/* Actions */}
                            <div className="flex gap-4">
                                <button
                                    onClick={onClose}
                                    className="flex-1 py-4 rounded-2xl border border-gray-200 font-bold text-gray-600 hover:bg-gray-50 transition-all"
                                >
                                    Hủy
                                </button>
                                <button
                                    onClick={handleCall}
                                    className="flex-1 py-4 rounded-2xl bg-[#EE501C] text-white font-bold shadow-lg shadow-orange-200 hover:bg-[#d44719] transition-all flex items-center justify-center gap-2"
                                >
                                    <Phone size={20} className="fill-white" /> Gọi ngay
                                </button>
                            </div>
                        </>
                    )}
                </div>
            </div>
        </Modal>
    );
};
