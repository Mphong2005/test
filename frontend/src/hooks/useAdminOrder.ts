/**
 * useAdminOrder Hook
 * Hook xử lý logic quản lý đơn hàng cho Admin
 * Tách từ OrderManagement.tsx
 */

import { useState, useEffect, useCallback, useMemo } from 'react';
import { Order } from '../types/common';
import { getAllOrdersApi, getOrdersByStatusApi, cancelOrderApi } from '../api/orderApi';

type OrderStatusFilter = 'All' | 'PENDING' | 'DELIVERING' | 'COMPLETED' | 'CANCELLED';

interface UseAdminOrderReturn {
    // Data
    orders: Order[];
    filteredOrders: Order[];

    // Filter & Search
    searchTerm: string;
    setSearchTerm: (term: string) => void;
    filterStatus: OrderStatusFilter;
    setFilterStatus: (status: OrderStatusFilter) => void;

    // Pagination
    currentPage: number;
    setCurrentPage: (page: number) => void;
    totalPages: number;
    paginatedOrders: Order[];

    // State
    loading: boolean;
    error: string | null;

    // Actions
    loadOrders: () => Promise<void>;
    loadOrdersByStatus: (status: string) => Promise<void>;
    cancelOrder: (orderId: string, reason?: string) => Promise<boolean>;

    // Helpers
    getStatusLabel: (status: string) => string;
    getStatusClasses: (status: string) => string;
}

const ITEMS_PER_PAGE = 7;

export const useAdminOrder = (): UseAdminOrderReturn => {
    // Data
    const [orders, setOrders] = useState<Order[]>([]);

    // Filter & Search
    const [searchTerm, setSearchTerm] = useState('');
    const [filterStatus, setFilterStatus] = useState<OrderStatusFilter>('All');

    // Pagination
    const [currentPage, setCurrentPage] = useState(1);

    // State
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    /**
     * Load all orders
     */
    const loadOrders = useCallback(async () => {
        try {
            setLoading(true);
            setError(null);
            const data = await getAllOrdersApi();
            setOrders(data);
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể tải đơn hàng';
            setError(message);
            console.error('Error loading orders:', err);
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Load orders by status
     */
    const loadOrdersByStatus = useCallback(async (status: string) => {
        try {
            setLoading(true);
            setError(null);
            const data = await getOrdersByStatusApi(status);
            setOrders(data);
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể tải đơn hàng';
            setError(message);
            console.error('Error loading orders by status:', err);
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Filter orders based on search and status
     */
    const filteredOrders = useMemo(() => {
        return orders.filter(o => {
            // Search filter
            const matchesSearch = searchTerm === '' ||
                o.id.toLowerCase().includes(searchTerm.toLowerCase()) ||
                o.restaurantName.toLowerCase().includes(searchTerm.toLowerCase()) ||
                (o.customer && o.customer.toLowerCase().includes(searchTerm.toLowerCase()));

            // Status filter
            const matchesStatus = filterStatus === 'All' || o.status === filterStatus;

            return matchesSearch && matchesStatus;
        });
    }, [orders, searchTerm, filterStatus]);

    /**
     * Pagination
     */
    const totalPages = Math.ceil(filteredOrders.length / ITEMS_PER_PAGE);

    const paginatedOrders = useMemo(() => {
        const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
        return filteredOrders.slice(startIndex, startIndex + ITEMS_PER_PAGE);
    }, [filteredOrders, currentPage]);

    // Reset page when filter changes
    useEffect(() => {
        setCurrentPage(1);
    }, [searchTerm, filterStatus]);

    /**
     * Cancel order (admin)
     */
    const cancelOrder = useCallback(async (orderId: string, reason?: string): Promise<boolean> => {
        try {
            await cancelOrderApi(orderId, reason);
            await loadOrders();
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể hủy đơn hàng';
            setError(message);
            console.error('Error canceling order:', err);
            return false;
        }
    }, [loadOrders]);

    /**
     * Get status label in Vietnamese
     */
    const getStatusLabel = useCallback((status: string): string => {
        const labels: Record<string, string> = {
            'PENDING': 'Đang chờ',
            'DELIVERING': 'Đang giao',
            'COMPLETED': 'Hoàn thành',
            'CANCELLED': 'Đã hủy',
        };
        return labels[status] || status;
    }, []);

    /**
     * Get status CSS classes
     */
    const getStatusClasses = useCallback((status: string): string => {
        const classes: Record<string, string> = {
            'PENDING': 'bg-yellow-100 text-yellow-700',
            'DELIVERING': 'bg-blue-100 text-blue-700',
            'COMPLETED': 'bg-green-100 text-green-700',
            'CANCELLED': 'bg-red-100 text-red-700',
        };
        return classes[status] || 'bg-gray-100 text-gray-700';
    }, []);

    // Load orders on mount
    useEffect(() => {
        loadOrders();
    }, [loadOrders]);

    return {
        orders,
        filteredOrders,
        searchTerm,
        setSearchTerm,
        filterStatus,
        setFilterStatus,
        currentPage,
        setCurrentPage,
        totalPages,
        paginatedOrders,
        loading,
        error,
        loadOrders,
        loadOrdersByStatus,
        cancelOrder,
        getStatusLabel,
        getStatusClasses,
    };
};
