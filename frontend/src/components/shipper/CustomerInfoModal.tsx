/**
 * CustomerInfoModal Component
 * Modal hiển thị thông tin khách hàng và cho phép gọi điện
 */

import React from 'react';
import { Modal } from '../common/Modal';
import { ShipperCustomer } from '../../types/shipper';
import { X, User, Phone, Mail, MapPin, Check } from 'lucide-react';
import { getInitials } from '../../utils';

interface CustomerInfoModalProps {
    isOpen: boolean;
    onClose: () => void;
    customer?: ShipperCustomer;
    deliveryAddress?: string;
}

export const CustomerInfoModal: React.FC<CustomerInfoModalProps> = ({
    isOpen,
    onClose,
    customer,
    deliveryAddress,
}) => {
    const handleCall = () => {
        const phoneNumber = customer?.phone?.replace(/\s/g, '') || '0901234567';
        window.location.href = `tel:${phoneNumber}`;
        onClose();
    };

    return (
        <Modal isOpen={isOpen} onClose={onClose} maxWidth="md" hideCloseButton={true}>
            <div className="flex flex-col -m-6 bg-white relative">
                {/* Header */}
                <div className="bg-[#EE501C] px-6 py-4 flex justify-between items-center">
                    <h3 className="text-white font-bold text-lg">Thông tin khách hàng</h3>
                    <button
                        onClick={onClose}
                        className="text-white/80 hover:text-white hover:bg-white/20 p-1.5 rounded-full transition-all"
                    >
                        <X size={20} />
                    </button>
                </div>

                {/* Body */}
                <div className="p-6">
                    {/* Profile Section (Horizontal) */}
                    <div className="flex items-center gap-4 mb-6">
                        <div className="w-16 h-16 bg-white rounded-full flex items-center justify-center text-[#EE501C] border-[3px] border-[#EE501C]/10 shadow-sm shrink-0 overflow-hidden">
                            {customer?.avatar ? (
                                <img
                                    src={customer.avatar}
                                    alt={customer.name}
                                    className="w-full h-full rounded-full object-cover"
                                />
                            ) : (
                                <User size={32} strokeWidth={2.5} />
                            )}
                        </div>
                        <div className="flex flex-col">
                            <h4 className="font-bold text-xl text-gray-900 leading-tight">
                                {customer?.name || 'Khách hàng'}
                            </h4>
                            {customer?.rank && (
                                <span className="inline-block bg-[#E8F5E9] text-[#2E7D32] text-[10px] font-bold px-2 py-1 rounded-md uppercase tracking-wide mt-1 w-fit">
                                    {customer.rank}
                                </span>
                            )}
                        </div>
                    </div>

                    {/* Details Fields */}
                    <div className="space-y-3">
                        <div className="flex items-center gap-4 px-4 py-3.5 border border-gray-200 rounded-[1.5rem] bg-white hover:border-gray-300 transition-colors">
                            <Phone size={20} className="text-[#EE501C] shrink-0" />
                            <div className="flex-1 min-w-0">
                                <p className="text-[10px] text-gray-400 font-bold uppercase tracking-wider mb-0.5">SỐ ĐIỆN THOẠI</p>
                                <p className="text-sm font-bold text-gray-900">{customer?.phone || '09xx xxx xxx'}</p>
                            </div>
                        </div>

                        <div className="flex items-center gap-4 px-4 py-3.5 border border-gray-200 rounded-[1.5rem] bg-white hover:border-gray-300 transition-colors">
                            <Mail size={20} className="text-[#EE501C] shrink-0" />
                            <div className="flex-1 min-w-0">
                                <p className="text-[10px] text-gray-400 font-bold uppercase tracking-wider mb-0.5">EMAIL</p>
                                <p className="text-sm font-bold text-gray-900 truncate">{customer?.email || 'N/A'}</p>
                            </div>
                        </div>

                        {deliveryAddress && (
                            <div className="flex items-start gap-4 px-4 py-3.5 border border-gray-200 rounded-[1.5rem] bg-white hover:border-gray-300 transition-colors">
                                <MapPin size={20} className="text-[#EE501C] shrink-0 mt-1" />
                                <div className="flex-1 min-w-0">
                                    <p className="text-[10px] text-gray-400 font-bold uppercase tracking-wider mb-0.5">ĐỊA CHỈ GIAO HÀNG</p>
                                    <p className="text-sm font-bold text-gray-900 leading-snug">
                                        {deliveryAddress}
                                    </p>
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Actions */}
                    <div className="flex gap-3 mt-6">
                        <button
                            onClick={onClose}
                            className="flex-1 py-3.5 rounded-full border border-gray-200 font-bold text-gray-600 hover:bg-gray-50 transition-all text-sm"
                        >
                            Hủy
                        </button>
                        <button
                            onClick={handleCall}
                            className="flex-1 py-3.5 rounded-full bg-[#EE501C] text-white font-bold shadow-lg shadow-orange-200 hover:bg-[#d44719] transition-all flex items-center justify-center gap-2 text-sm"
                        >
                            <Phone size={18} className="fill-white" /> Gọi ngay
                        </button>
                    </div>
                </div>
            </div>
        </Modal>
    );
};
