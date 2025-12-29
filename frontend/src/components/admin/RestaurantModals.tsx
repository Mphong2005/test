import React, { useEffect, useState } from 'react';
import { Modal } from '../common/Modal';
import { Button } from '../common/Button';
import { Restaurant, FoodItem } from '../../types/common';
import { getRestaurantMenuApi } from '../../api/restaurantApi';
import { Star, X } from 'lucide-react';
import { DeleteConfirmModal } from '../common/DeleteConfirmModal';
import { getInitials, formatNumber } from '../../utils';

interface RestaurantModalsProps {
    modalType: 'MENU' | 'DELETE' | null;
    restaurant: Restaurant | null;
    onClose: () => void;
    onConfirmDelete: () => void;
}

export const RestaurantModals: React.FC<RestaurantModalsProps> = ({
    modalType,
    restaurant,
    onClose,
    onConfirmDelete
}) => {
    const [foods, setFoods] = useState<FoodItem[]>([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (modalType === 'MENU' && restaurant) {
            const fetchMenu = async () => {
                setLoading(true);
                try {
                    const menu = await getRestaurantMenuApi(restaurant.id);
                    console.log('Menu fetched:', menu);
                    console.log('Menu length:', menu?.length);
                    setFoods(menu || []);
                } catch (error) {
                    console.error("Failed to load menu", error);
                } finally {
                    setLoading(false);
                }
            };
            fetchMenu();
        } else {
            setFoods([]);
        }
    }, [modalType, restaurant]);

    // Helper to map category to Vietnamese label
    const getCategoryLabel = (cat: string) => {
        const map: Record<string, string> = {
            'fastfood': 'Đồ ăn nhanh',
            'noodles': 'Món nước',
            'rice': 'Cơm',
            'pizza': 'Pizza',
            'burger': 'Burger',
            'drink': 'Đồ uống',
            'snack': 'Ăn vặt',
            'sushi': 'Sushi',
            'dessert': 'Tráng miệng'
        };
        return map[cat] || cat || 'Khác';
    };

    // Handle Loading for Menu
    if (loading && modalType === 'MENU') {
        return (
            <Modal isOpen={true} onClose={onClose} maxWidth="5xl">
                <div className="flex justify-center items-center py-40">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#EE501C]"></div>
                </div>
            </Modal>
        );
    }

    return (
        <>
            {/* MENU MODAL */}
            {/* Remove title prop to hide default header, we use custom header below */}
            <Modal isOpen={modalType === 'MENU'} onClose={onClose} maxWidth="5xl">
                {restaurant && (
                    <div className="flex flex-col h-[80vh] -m-6"> {/* Negative margin to counteract Modal padding */}

                        {/* Custom Header */}
                        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between bg-white z-10 shrink-0">
                            <div className="flex items-center gap-4">
                                <div className="w-12 h-12 rounded-xl bg-red-100 flex items-center justify-center text-red-600 font-bold text-xl shadow-sm">
                                    {restaurant.initial || getInitials(restaurant.name)}
                                </div>
                                <div>
                                    <h2 className="text-xl font-bold text-gray-900">Menu: {restaurant.name}</h2>
                                    <div className="flex items-center gap-2 text-sm text-gray-500">
                                        <span className="flex items-center gap-1"><Star size={16} className="fill-yellow-400 text-yellow-400" /> {restaurant.rating}</span>
                                        <span>•</span>
                                        <span>{restaurant.category}</span>
                                        <span>•</span>
                                        <span className="text-green-600 font-medium">Đang hoạt động</span>
                                    </div>
                                </div>
                            </div>
                            <button
                                onClick={onClose}
                                className="w-8 h-8 rounded-full bg-gray-100 text-gray-500 hover:bg-gray-200 flex items-center justify-center transition-colors"
                            >
                                <X size={20} />
                            </button>
                        </div>

                        {/* Scrollable Table Body */}
                        <div className="flex-1 overflow-y-auto bg-gray-50">
                            <table className="w-full text-left border-collapse">
                                <thead className="bg-gray-50 text-xs uppercase text-gray-500 font-semibold sticky top-0 z-10 border-b border-gray-200 shadow-sm">
                                    <tr>
                                        <th className="px-6 py-3 bg-gray-50">Ảnh</th>
                                        <th className="px-6 py-3 bg-gray-50">Tên món</th>
                                        <th className="px-6 py-3 bg-gray-50">Loại món ăn</th>
                                        <th className="px-6 py-3 bg-gray-50">Giá tiền</th>
                                        <th className="px-6 py-3 bg-gray-50 text-center">Trạng thái</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-200 text-sm bg-white">
                                    {foods.length > 0 ? foods.map(food => (
                                        <tr key={food.id} className="hover:bg-gray-50 transition-colors group">
                                            <td className="px-6 py-3 w-24">
                                                <div className="w-16 h-16 rounded-lg overflow-hidden border border-gray-200 relative bg-white">
                                                    <img src={food.imageUrl} alt={food.name} className="w-full h-full object-cover" />
                                                </div>
                                            </td>
                                            <td className="px-6 py-3">
                                                <div className="font-medium text-gray-900 text-base">{food.name}</div>
                                                <div className="text-xs text-gray-500 mt-0.5 line-clamp-1">{food.description || 'Món ngon đặc biệt của quán'}</div>
                                            </td>
                                            <td className="px-6 py-3 text-gray-500">
                                                {getCategoryLabel(food.category)}
                                            </td>
                                            <td className="px-6 py-3">
                                                <div className="font-bold text-[#EE501C] text-base">{formatNumber(food.price)}đ</div>
                                            </td>
                                            <td className="px-6 py-3 text-center">
                                                <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                                                    Còn hàng
                                                </span>
                                            </td>
                                        </tr>
                                    )) : (
                                        <tr>
                                            <td colSpan={5} className="py-20 text-center text-gray-400">
                                                Nhà hàng chưa cập nhật menu.
                                            </td>
                                        </tr>
                                    )}
                                </tbody>
                            </table>
                        </div>

                        {/* Footer */}
                        <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center shrink-0">
                            <span className="text-sm text-gray-500">Đang hiển thị {foods.length} món ăn</span>
                            <div className="flex gap-3">
                                <button
                                    onClick={onClose}
                                    className="px-4 py-2 bg-white border border-gray-300 text-gray-700 rounded-lg text-sm font-medium hover:bg-gray-50 shadow-sm transition-colors"
                                >
                                    Đóng
                                </button>
                            </div>
                        </div>
                    </div>
                )}
            </Modal>

            {/* DELETE CONFIRMATION MODAL - sử dụng DeleteConfirmModal component */}
            <DeleteConfirmModal
                isOpen={modalType === 'DELETE'}
                onClose={onClose}
                onConfirm={onConfirmDelete}
                title="Xác nhận xóa nhà hàng"
                itemName={restaurant?.name || ''}
                description={`Bạn có chắc chắn muốn xóa nhà hàng ${restaurant?.name || ''} không? Hành động này sẽ xóa vĩnh viễn dữ liệu và không thể hoàn tác.`}
            />
        </>
    );
};