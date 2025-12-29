/**
 * Cart Context
 * Context để quản lý giỏ hàng toàn ứng dụng
 */

import React, { createContext, useContext, useState, useCallback, useMemo } from 'react';
import { FoodItem } from '../types/common';

// Cart item interface
export interface CartItem {
    food: FoodItem;
    quantity: number;
}

// Cart context type
export interface CartContextType {
    // State
    cartItems: CartItem[];
    restaurantId: string | null;

    // Computed
    subtotal: number;
    itemCount: number;
    isEmpty: boolean;

    // Actions
    addItem: (food: FoodItem, quantity?: number) => void;
    removeItem: (foodId: string) => void;
    updateQuantity: (foodId: string, quantity: number) => void;
    clearCart: () => void;
    setItems: (items: CartItem[]) => void;
}

// Create context
const CartContext = createContext<CartContextType | undefined>(undefined);

// Provider component
export const CartProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const [cartItems, setCartItems] = useState<CartItem[]>([]);

    // Get restaurantId from first item (all items should be from same restaurant)
    const restaurantId = useMemo(() => {
        return cartItems.length > 0 ? cartItems[0].food.restaurantId || null : null;
    }, [cartItems]);

    // Calculate subtotal
    const subtotal = useMemo(() => {
        return cartItems.reduce((sum, item) => sum + (item.food.price * item.quantity), 0);
    }, [cartItems]);

    // Calculate item count
    const itemCount = useMemo(() => {
        return cartItems.reduce((sum, item) => sum + item.quantity, 0);
    }, [cartItems]);

    // Check if cart is empty
    const isEmpty = cartItems.length === 0;

    /**
     * Add item to cart
     * If item already exists, increase quantity
     * If item from different restaurant, clear cart first
     */
    const addItem = useCallback((food: FoodItem, quantity: number = 1) => {
        setCartItems(prev => {
            // Check if adding from different restaurant
            if (prev.length > 0 && prev[0].food.restaurantId !== food.restaurantId) {
                // Clear cart and add new item
                return [{ food, quantity }];
            }

            // Check if item already exists in cart
            const existingIndex = prev.findIndex(item => item.food.id === food.id);
            if (existingIndex >= 0) {
                // Update quantity if exists
                const newItems = [...prev];
                newItems[existingIndex].quantity += quantity;
                return newItems;
            } else {
                // Add new item
                return [...prev, { food, quantity }];
            }
        });
    }, []);

    /**
     * Remove item from cart
     */
    const removeItem = useCallback((foodId: string) => {
        setCartItems(prev => prev.filter(item => item.food.id !== foodId));
    }, []);

    /**
     * Update item quantity
     * If quantity <= 0, remove item
     */
    const updateQuantity = useCallback((foodId: string, quantity: number) => {
        if (quantity <= 0) {
            setCartItems(prev => prev.filter(item => item.food.id !== foodId));
            return;
        }
        setCartItems(prev =>
            prev.map(item =>
                item.food.id === foodId ? { ...item, quantity } : item
            )
        );
    }, []);

    /**
     * Clear all items from cart
     */
    const clearCart = useCallback(() => {
        setCartItems([]);
    }, []);

    /**
     * Set cart items directly (for initialization from navigation state)
     */
    const setItems = useCallback((items: CartItem[]) => {
        setCartItems(items);
    }, []);

    const value: CartContextType = {
        cartItems,
        restaurantId,
        subtotal,
        itemCount,
        isEmpty,
        addItem,
        removeItem,
        updateQuantity,
        clearCart,
        setItems,
    };

    return (
        <CartContext.Provider value={value}>
            {children}
        </CartContext.Provider>
    );
};

/**
 * Hook to use cart context
 */
export const useCart = (): CartContextType => {
    const context = useContext(CartContext);
    if (context === undefined) {
        throw new Error('useCart must be used within a CartProvider');
    }
    return context;
};

/**
 * Hook to use cart context with optional fallback (for pages that may not need cart)
 */
export const useCartOptional = (): CartContextType | null => {
    const context = useContext(CartContext);
    return context ?? null;
};
