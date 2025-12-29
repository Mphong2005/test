/**
 * useShipperOrder Hook
 * Hook xử lý logic quản lý đơn hàng cho Shipper
 * Tách từ ShipperHomePage.tsx, ShipperPendingPage.tsx, ShipperHistoryPage.tsx
 */

import { useState, useEffect, useCallback, useMemo } from 'react';
import { ShipperOrder, OrderStatus } from '../types/shipper';
import {
    getShipperOrdersApi,
    acceptOrderApi,
    completeOrderApi,
    declineOrderApi,
    rejectOrderApi,
    getShipperStatsApi,
} from '../api/shipperApi';

interface ShipperStats {
    todayIncome: number;
    completedCount: number;
    activeHours: string;
}

interface UseShipperOrderReturn {
    // Data
    orders: ShipperOrder[];
    stats: ShipperStats;

    // Filter
    activeTab: 'active' | 'pending' | 'history';
    setActiveTab: (tab: 'active' | 'pending' | 'history') => void;
    historyFilter: 'All' | OrderStatus.Completed | OrderStatus.Cancelled;
    setHistoryFilter: (filter: 'All' | OrderStatus.Completed | OrderStatus.Cancelled) => void;

    // Filtered orders
    activeOrders: ShipperOrder[];
    pendingOrders: ShipperOrder[];
    historyOrders: ShipperOrder[];

    // State
    loading: boolean;
    error: string | null;

    // Actions
    loadOrders: (status?: OrderStatus | 'HISTORY') => Promise<void>;
    loadStats: () => Promise<void>;
    acceptOrder: (orderId: string) => Promise<boolean>;
    completeOrder: (orderId: string) => Promise<boolean>;
    declineOrder: (orderId: string, reason?: string) => Promise<boolean>;
    rejectOrder: (orderId: string, reason?: string) => Promise<boolean>;
    refreshAll: () => Promise<void>;
}

export const useShipperOrder = (): UseShipperOrderReturn => {
    // Data
    const [orders, setOrders] = useState<ShipperOrder[]>([]);
    const [stats, setStats] = useState<ShipperStats>({
        todayIncome: 0,
        completedCount: 0,
        activeHours: '0h 0p',
    });

    // Filter
    const [activeTab, setActiveTab] = useState<'active' | 'pending' | 'history'>('active');
    const [historyFilter, setHistoryFilter] = useState<'All' | OrderStatus.Completed | OrderStatus.Cancelled>('All');

    // State
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    /**
     * Load orders by status
     */
    const loadOrders = useCallback(async (status?: OrderStatus | 'HISTORY') => {
        try {
            setLoading(true);
            setError(null);
            const data = await getShipperOrdersApi(status);
            setOrders(data);
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể tải đơn hàng';
            setError(message);
            console.error('Error loading shipper orders:', err);
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Load shipper stats
     */
    const loadStats = useCallback(async () => {
        try {
            const data = await getShipperStatsApi();
            setStats(data);
        } catch (err) {
            console.error('Error loading shipper stats:', err);
        }
    }, []);

    /**
     * Filter orders - Active (Delivering)
     */
    const activeOrders = useMemo(() => {
        return orders.filter(o => o.status === OrderStatus.Delivering);
    }, [orders]);

    /**
     * Filter orders - Pending
     */
    const pendingOrders = useMemo(() => {
        return orders.filter(o => o.status === OrderStatus.Pending);
    }, [orders]);

    /**
     * Filter orders - History (Completed/Cancelled)
     */
    const historyOrders = useMemo(() => {
        return orders.filter(o => {
            if (historyFilter === 'All') {
                return o.status === OrderStatus.Completed || o.status === OrderStatus.Cancelled;
            }
            return o.status === historyFilter;
        });
    }, [orders, historyFilter]);

    /**
     * Accept order
     */
    const acceptOrder = useCallback(async (orderId: string): Promise<boolean> => {
        try {
            const success = await acceptOrderApi(orderId);
            if (success) {
                await loadOrders(OrderStatus.Pending);
                await loadStats();
            }
            return success;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể nhận đơn hàng';
            setError(message);
            console.error('Error accepting order:', err);
            return false;
        }
    }, [loadOrders, loadStats]);

    /**
     * Complete order
     */
    const completeOrder = useCallback(async (orderId: string): Promise<boolean> => {
        try {
            const success = await completeOrderApi(orderId);
            if (success) {
                await loadOrders(OrderStatus.Delivering);
                await loadStats();
            }
            return success;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể hoàn thành đơn hàng';
            setError(message);
            console.error('Error completing order:', err);
            return false;
        }
    }, [loadOrders, loadStats]);

    /**
     * Decline order (chưa nhận)
     */
    const declineOrder = useCallback(async (orderId: string, reason?: string): Promise<boolean> => {
        try {
            const success = await declineOrderApi(orderId, reason);
            if (success) {
                await loadOrders(OrderStatus.Pending);
            }
            return success;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể từ chối đơn hàng';
            setError(message);
            console.error('Error declining order:', err);
            return false;
        }
    }, [loadOrders]);

    /**
     * Reject order (đã nhận)
     */
    const rejectOrder = useCallback(async (orderId: string, reason?: string): Promise<boolean> => {
        try {
            const success = await rejectOrderApi(orderId, reason);
            if (success) {
                await loadOrders(OrderStatus.Delivering);
            }
            return success;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể từ chối đơn hàng';
            setError(message);
            console.error('Error rejecting order:', err);
            return false;
        }
    }, [loadOrders]);

    /**
     * Refresh all data
     */
    const refreshAll = useCallback(async () => {
        await Promise.all([loadOrders(), loadStats()]);
    }, [loadOrders, loadStats]);

    // Load orders and stats on mount
    useEffect(() => {
        refreshAll();
    }, [refreshAll]);

    return {
        orders,
        stats,
        activeTab,
        setActiveTab,
        historyFilter,
        setHistoryFilter,
        activeOrders,
        pendingOrders,
        historyOrders,
        loading,
        error,
        loadOrders,
        loadStats,
        acceptOrder,
        completeOrder,
        declineOrder,
        rejectOrder,
        refreshAll,
    };
};
