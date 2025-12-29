/**
 * Cart Service
 * File này chứa các nghiệp vụ liên quan đến giỏ hàng
 * Bao gồm transform data và business logic
 */

/**
 * Cart item interface for frontend
 */
export interface CartItem {
    foodName: string;
    quantity: number;
    unitPrice: number;
    totalPrice: number;
    image?: string;
}

/**
 * Cart interface for frontend
 */
export interface Cart {
    id: string;
    userId: string;
    restaurantId: string;
    restaurantName: string;
    items: CartItem[];
    totalAmount: number;
    itemCount: number;
}

/**
 * Transform backend cart item to frontend format
 */
export function transformCartItem(item: any): CartItem {
    return {
        foodName: item.food_name || item.foodName || '',
        quantity: item.quantity || 0,
        unitPrice: item.unit_price || item.unitPrice || 0,
        totalPrice: item.total_price || item.totalPrice || (item.quantity * (item.unit_price || item.unitPrice || 0)),
        image: item.image || item.imageUrl,
    };
}

/**
 * Transform backend cart to frontend format
 */
export function transformCart(cart: any): Cart | null {
    if (!cart) return null;

    const items = (cart.items || []).map((item: any) => transformCartItem(item));

    return {
        id: cart._id || cart.id || '',
        userId: cart.user_id || cart.userId || '',
        restaurantId: cart.restaurant_id || cart.restaurantId || '',
        restaurantName: cart.restaurant_name || cart.restaurantName || '',
        items,
        totalAmount: cart.total_amount || cart.totalAmount || calculateCartTotal(items),
        itemCount: items.reduce((sum: number, item: CartItem) => sum + item.quantity, 0),
    };
}

/**
 * Calculate cart total from items
 */
export function calculateCartTotal(items: CartItem[]): number {
    return items.reduce((sum, item) => sum + item.totalPrice, 0);
}

/**
 * Calculate cart item count
 */
export function calculateCartItemCount(items: CartItem[]): number {
    return items.reduce((sum, item) => sum + item.quantity, 0);
}

/**
 * Check if cart is empty
 */
export function isCartEmpty(cart: Cart | null): boolean {
    return !cart || cart.items.length === 0;
}

/**
 * Check if adding item from different restaurant
 */
export function isDifferentRestaurant(cart: Cart | null, restaurantId: string): boolean {
    if (!cart || cart.items.length === 0) return false;
    return cart.restaurantId !== restaurantId;
}
