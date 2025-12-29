/**
 * ReviewModal Component
 * Modal cho user đánh giá đơn hàng
 */

import React, { useState } from 'react';
import { Star } from 'lucide-react';
import { Modal } from '../common/Modal';
import { Order } from '../../types/common';

interface ReviewModalProps {
    isOpen: boolean;
    onClose: () => void;
    order: Order | null;
    onSubmit: (rating: number, comment: string) => Promise<void>;
}

const RATING_LABELS: Record<number, string> = {
    5: 'Rất hài lòng',
    4: 'Hài lòng',
    3: 'Bình thường',
    2: 'Không hài lòng',
    1: 'Rất tệ',
};

export const ReviewModal: React.FC<ReviewModalProps> = ({
    isOpen,
    onClose,
    order,
    onSubmit,
}) => {
    const [rating, setRating] = useState(5);
    const [hoverRating, setHoverRating] = useState(0);
    const [comment, setComment] = useState('');
    const [isSubmitting, setIsSubmitting] = useState(false);

    const handleSubmit = async () => {
        if (!order || comment.length < 5) return;

        setIsSubmitting(true);
        try {
            await onSubmit(rating, comment);
            // Reset form
            setRating(5);
            setComment('');
        } finally {
            setIsSubmitting(false);
        }
    };

    const handleClose = () => {
        setRating(5);
        setHoverRating(0);
        setComment('');
        onClose();
    };

    if (!order) return null;

    return (
        <Modal isOpen={isOpen} onClose={handleClose} title="Đánh giá sản phẩm" maxWidth="lg">
            <div className="flex flex-col gap-6">
                {/* Product Info */}
                <div className="flex gap-4">
                    <div className="w-16 h-16 rounded-xl overflow-hidden shrink-0 border border-gray-100">
                        <img src={order.imageUrl} alt="" className="w-full h-full object-cover" />
                    </div>
                    <div>
                        <h4 className="font-bold text-gray-900 text-base mb-1">{order.restaurantName}</h4>
                        <p className="text-xs text-gray-500 line-clamp-2">{order.description}</p>
                    </div>
                </div>

                {/* Rating */}
                <div className="flex flex-col items-center justify-center py-4 gap-3 border-y border-dashed border-gray-100">
                    <p className="text-sm font-medium text-gray-600">Vui lòng đánh giá chất lượng món ăn</p>
                    <div className="flex gap-3">
                        {[1, 2, 3, 4, 5].map((star) => (
                            <button
                                key={star}
                                type="button"
                                onMouseEnter={() => setHoverRating(star)}
                                onMouseLeave={() => setHoverRating(0)}
                                onClick={() => setRating(star)}
                                className="focus:outline-none transition-transform active:scale-95"
                            >
                                <Star
                                    size={32}
                                    className={`transition-all ${star <= (hoverRating || rating)
                                        ? 'text-[#EE501C] fill-[#EE501C] scale-110 drop-shadow-sm'
                                        : 'text-gray-200'
                                        }`}
                                />
                            </button>
                        ))}
                    </div>
                    <span className="text-[#EE501C] font-bold text-sm h-5">
                        {RATING_LABELS[hoverRating || rating] || ''}
                    </span>
                </div>

                {/* Comment */}
                <div>
                    <label className="text-sm font-bold text-gray-900 mb-2 block">Nhận xét của bạn</label>
                    <textarea
                        value={comment}
                        onChange={(e) => setComment(e.target.value)}
                        placeholder="Hãy chia sẻ nhận xét cho món ăn này nhé! (Tối thiểu 5 ký tự)"
                        className="w-full h-32 p-4 rounded-2xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-[#EE501C] focus:ring-4 focus:ring-orange-50 outline-none transition-all text-sm resize-none placeholder:text-gray-400"
                    />
                </div>

                {/* Actions */}
                <div className="flex gap-4 pt-2">
                    <button
                        onClick={handleClose}
                        className="flex-1 py-3.5 rounded-xl bg-gray-50 text-gray-600 font-bold hover:bg-gray-100 transition-all text-sm"
                    >
                        Trở lại
                    </button>
                    <button
                        onClick={handleSubmit}
                        disabled={comment.length < 5 || isSubmitting}
                        className="flex-1 py-3.5 rounded-xl bg-[#EE501C] text-white font-bold shadow-lg shadow-orange-200 hover:bg-[#d44719] transition-all text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        {isSubmitting ? 'Đang gửi...' : 'Gửi đánh giá'}
                    </button>
                </div>
            </div>
        </Modal>
    );
};
