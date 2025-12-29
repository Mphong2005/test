/**
 * ShipperOrderCard Component
 * Card hiển thị thông tin đơn hàng trong lịch sử shipper
 */

import React from 'react';
import { ShipperOrder, OrderStatus } from '../../types/shipper';
import { MapPin } from 'lucide-react';

interface ShipperOrderCardProps {
    order: ShipperOrder;
    onClick?: () => void;
}

export const ShipperOrderCard: React.FC<ShipperOrderCardProps> = ({ order, onClick }) => (
    <div
        onClick={onClick}
        className="bg-white border border-[#f0e4e0] rounded-[2.5rem] p-5 flex items-center gap-6 shadow-sm hover:shadow-md transition-all group cursor-pointer"
    >
        <div
            className={`w-20 h-20 rounded-[1.5rem] bg-cover bg-center shrink-0 shadow-sm border border-gray-100 ${order.status === OrderStatus.Cancelled ? 'grayscale opacity-70' : ''}`}
            style={{ backgroundImage: `url("${order.storeImage}")` }}
        />

        <div className="flex-1 min-w-0 flex flex-col gap-2">
            <div className="flex items-center gap-3">
                <h4 className="text-base font-bold text-gray-900 truncate">{order.foodName || order.storeName}</h4>
                <span className={`px-2.5 py-1 rounded-full text-[10px] font-bold uppercase tracking-wide ${order.status === OrderStatus.Completed
                        ? 'bg-[#e8fbf1] text-[#1db954]'
                        : 'bg-gray-100 text-gray-500'
                    }`}>
                    {order.status === OrderStatus.Completed ? 'HOÀN THÀNH' : 'ĐÃ HỦY'}
                </span>
            </div>

            <div className="flex items-center gap-1.5 text-gray-500">
                <MapPin size={16} className="text-[#EE501C]" />
                <p className="text-sm font-medium truncate opacity-90">{order.storeAddress}</p>
            </div>

            <p className="text-[11px] text-gray-400 font-medium">
                Mã đơn: <span className="font-mono text-gray-600">{order.id}</span> • {order.status === OrderStatus.Completed ? 'Giao lúc' : 'Hủy lúc'} {order.time.split('-')[0]}
            </p>
        </div>

        <div className="w-px h-12 bg-gray-100 mx-2 hidden md:block"></div>

        <div className="text-right flex flex-col items-end pr-2 min-w-[100px]">
            <p className={`text-2xl font-black leading-tight ${order.status === OrderStatus.Cancelled ? 'text-gray-400' : 'text-[#EE501C]'}`}>
                {order.totalAmount.toLocaleString('vi-VN')}đ
            </p>
            <p className="text-[11px] text-gray-400 font-medium mt-1">
                {order.status === OrderStatus.Cancelled ? 'Khách hủy' : (order.paymentMethod === 'Cash' ? 'Tiền mặt' : 'Ví điện tử')}
            </p>
        </div>
    </div>
);
