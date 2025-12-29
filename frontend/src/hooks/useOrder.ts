/**
 * useOrder Hook
 * Hook xử lý logic quản lý đơn hàng cho User
 * Tách từ OrdersPage.tsx
 */

import { useState, useEffect, useCallback, useMemo } from 'react';
import { Order } from '../types/common';
import { getMyOrdersApi, userCancelOrderApi } from '../api/orderApi';
import { submitReviewApi } from '../api/reviewApi';

export type OrderTab = 'all' | 'pending' | 'delivering' | 'completed' | 'review';

interface UseOrderReturn {
    // Data
    orders: Order[];
    filteredOrders: Order[];

    // Filter
    activeTab: OrderTab;
    setActiveTab: (tab: OrderTab) => void;

    // State
    loading: boolean;
    error: string | null;

    // Actions
    fetchOrders: () => Promise<void>;
    cancelOrder: (orderId: string, reason?: string) => Promise<boolean>;
    submitReview: (orderId: string, rating: number, comment?: string) => Promise<boolean>;

    // Helpers
    getOrdersByStatus: (status: string) => Order[];
    getOrdersNeedingReview: () => Order[];
}

export const useOrder = (): UseOrderReturn => {
    const [orders, setOrders] = useState<Order[]>([]);
    const [activeTab, setActiveTab] = useState<OrderTab>('all');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    /**
     * Fetch orders from API
     */
    const fetchOrders = useCallback(async () => {
        try {
            setLoading(true);
            setError(null);
            const data = await getMyOrdersApi();
            setOrders(data);
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể tải đơn hàng';
            setError(message);
            console.error('Error fetching orders:', err);
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Cancel order
     */
    const cancelOrder = useCallback(async (orderId: string, reason?: string): Promise<boolean> => {
        try {
            await userCancelOrderApi(orderId, reason);
            await fetchOrders(); // Refresh orders
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể hủy đơn hàng';
            setError(message);
            console.error('Error canceling order:', err);
            return false;
        }
    }, [fetchOrders]);

    /**
     * Submit review for an order
     */
    const submitReview = useCallback(async (
        orderId: string,
        rating: number,
        comment?: string
    ): Promise<boolean> => {
        try {
            await submitReviewApi({ orderId, rating, comment });
            await fetchOrders(); // Refresh to update review status
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể gửi đánh giá';
            setError(message);
            console.error('Error submitting review:', err);
            return false;
        }
    }, [fetchOrders]);

    /**
     * Filter orders by status
     */
    const getOrdersByStatus = useCallback((status: string): Order[] => {
        return orders.filter(order => order.status === status);
    }, [orders]);

    /**
     * Get orders that need review
     */
    const getOrdersNeedingReview = useCallback((): Order[] => {
        return orders.filter(order => order.needsReview && !order.isReviewed);
    }, [orders]);

    /**
     * Filtered orders based on active tab
     */
    const filteredOrders = useMemo(() => {
        switch (activeTab) {
            case 'pending':
                return orders.filter(o => o.status === 'PENDING');
            case 'delivering':
                return orders.filter(o => o.status === 'DELIVERING');
            case 'completed':
                return orders.filter(o => o.status === 'COMPLETED');
            case 'review':
                return orders.filter(o => o.needsReview && !o.isReviewed);
            case 'all':
            default:
                return orders;
        }
    }, [orders, activeTab]);

    // Load orders on mount
    useEffect(() => {
        fetchOrders();
    }, [fetchOrders]);

    return {
        orders,
        filteredOrders,
        activeTab,
        setActiveTab,
        loading,
        error,
        fetchOrders,
        cancelOrder,
        submitReview,
        getOrdersByStatus,
        getOrdersNeedingReview,
    };
};
