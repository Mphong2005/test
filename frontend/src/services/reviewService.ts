/**
 * Review Service
 * File này chứa các nghiệp vụ liên quan đến đánh giá
 * Bao gồm transform data từ backend sang frontend format
 */

import { Review } from '../types/common';

/**
 * Transform backend review data to frontend Review format
 */
export function transformReview(review: any, foodId: string): Review {
    return {
        id: review._id || review.id || String(Math.random()),
        userId: review.userId || review.user_id || '',
        userName: review.userFullname || review.userName || 'Anonymous',
        foodId: foodId,
        rating: review.rating || 0,
        comment: review.comment || '',
        date: review.createdAt || review.created_at || new Date().toISOString(),
    };
}

/**
 * Transform an array of reviews
 */
export function transformReviews(reviews: any[], foodId: string): Review[] {
    return reviews.map(review => transformReview(review, foodId));
}

/**
 * Prepare review data for submission to backend
 */
export function prepareReviewSubmission(data: {
    orderId: string;
    rating: number;
    comment?: string;
}): { orderId: string; rating: number; comment: string } {
    return {
        orderId: data.orderId,
        rating: data.rating,
        comment: data.comment || '',
    };
}

/**
 * Calculate average rating from reviews
 */
export function calculateAverageRating(reviews: Review[]): number {
    if (reviews.length === 0) return 0;
    const total = reviews.reduce((sum, review) => sum + review.rating, 0);
    return Math.round((total / reviews.length) * 10) / 10;
}

/**
 * Get rating distribution from reviews
 */
export function getRatingDistribution(reviews: Review[]): Record<number, number> {
    const distribution: Record<number, number> = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
    reviews.forEach(review => {
        const rating = Math.round(review.rating);
        if (rating >= 1 && rating <= 5) {
            distribution[rating]++;
        }
    });
    return distribution;
}
