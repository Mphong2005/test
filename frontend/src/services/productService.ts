/**
 * Product Service
 * File này chứa các nghiệp vụ liên quan đến sản phẩm/món ăn
 * Bao gồm transform data từ backend sang frontend format
 */

import { FoodItem } from '../types/common';

/**
 * Default placeholder image for food items without images
 */
const DEFAULT_FOOD_IMAGE = 'https://res.cloudinary.com/dvobb8q7p/image/upload/v1764461748/Screenshot_2025-11-30_070923_miqawt.png';

/**
 * Transform backend food data to frontend FoodItem format
 */
export function transformFoodItem(food: any): FoodItem {
    return {
        id: food.id || String(Math.random()),
        name: food.name || 'Món ăn',
        price: food.price || 0,
        description: food.description || '',
        imageUrl: food.imageUrl || food.image || '',
        category: food.category || '',
        restaurantId: food.restaurantId || '',
        rating: food.rating || 4.0,
        distance: food.distance || '1.5',
        deliveryTime: food.deliveryTime || '15-20 phút',
        originalPrice: food.originalPrice,
        promoTag: food.promoTag,
    };
}

/**
 * Transform an array of food items
 */
export function transformFoodItems(foods: any[]): FoodItem[] {
    return foods.map(food => transformFoodItem(food));
}

/**
 * Transform backend category data to frontend format
 */
export function transformCategory(category: any): { id: string; name: string; image: string } {
    return {
        id: category.id || category.name?.toLowerCase().replace(/\s+/g, '-') || String(Math.random()),
        name: category.name || 'Danh mục',
        image: category.image || DEFAULT_FOOD_IMAGE,
    };
}

/**
 * Transform an array of categories
 */
export function transformCategories(categories: any[]): { id: string; name: string; image: string }[] {
    return categories.map(cat => transformCategory(cat));
}

/**
 * Transform backend promotion data to frontend format
 */
export function transformPromotion(promo: any): {
    id: string;
    name: string;
    vendor: string;
    image: string;
    action: string;
    foodId: string;
} {
    return {
        id: promo.id || promo._id || String(Math.random()),
        name: promo.foodName || promo.name || 'Món ăn',
        vendor: promo.restaurantName || promo.vendor || 'Nhà hàng',
        image: promo.image || promo.imageUrl || 'https://images.unsplash.com/photo-1513104890138-7c749659a591?q=80&w=800',
        action: promo.action || 'Khuyến mãi',
        foodId: promo.foodId || promo.id || String(Math.random()),
    };
}

/**
 * Transform an array of promotions and ensure minimum count
 */
export function transformPromotions(promotions: any[], minCount: number = 8): ReturnType<typeof transformPromotion>[] {
    const transformed = promotions.map(promo => transformPromotion(promo));

    // Đảm bảo có đủ promotions (lặp lại nếu cần)
    while (transformed.length < minCount && transformed.length > 0) {
        transformed.push(...transformed.slice(0, minCount - transformed.length));
    }

    return transformed.slice(0, minCount);
}

/**
 * Transform categories to suggestions format
 */
export function transformToSuggestions(categories: any[], maxCount: number = 8): { name: string; image: string; tag: string }[] {
    return categories.slice(0, maxCount).map((cat: any) => ({
        name: cat.name,
        image: cat.image,
        tag: 'Danh mục phổ biến'
    }));
}
