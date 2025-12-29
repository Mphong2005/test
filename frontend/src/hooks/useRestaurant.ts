/**
 * useRestaurant Hook
 * Hook xử lý logic quản lý nhà hàng cho Admin
 * Tách từ RestaurantManagement.tsx
 */

import { useState, useEffect, useCallback, useMemo } from 'react';
import { Restaurant } from '../types/common';
import {
    getRestaurantsApi,
    createRestaurantApi,
    updateRestaurantApi,
    deleteRestaurantApi,
    updateRestaurantStatusApi,
} from '../api/restaurantApi';

type RestaurantFilter = 'All' | 'Active' | 'Inactive';

interface UseRestaurantReturn {
    // Data
    restaurants: Restaurant[];
    filteredRestaurants: Restaurant[];

    // Filter & Search
    searchTerm: string;
    setSearchTerm: (term: string) => void;
    filterStatus: RestaurantFilter;
    setFilterStatus: (status: RestaurantFilter) => void;

    // Pagination
    currentPage: number;
    setCurrentPage: (page: number) => void;
    totalPages: number;
    paginatedRestaurants: Restaurant[];

    // State
    loading: boolean;
    error: string | null;

    // Modal
    selectedRestaurant: Restaurant | null;
    setSelectedRestaurant: (restaurant: Restaurant | null) => void;
    showDeleteModal: boolean;
    setShowDeleteModal: (show: boolean) => void;
    showMenuModal: boolean;
    setShowMenuModal: (show: boolean) => void;

    // Actions
    loadRestaurants: () => Promise<void>;
    createRestaurant: (data: any) => Promise<boolean>;
    updateRestaurant: (id: string, data: any) => Promise<boolean>;
    deleteRestaurant: (id: string) => Promise<boolean>;
    toggleStatus: (id: string, currentStatus: string) => Promise<boolean>;
}

const ITEMS_PER_PAGE = 7;

export const useRestaurant = (): UseRestaurantReturn => {
    // Data
    const [restaurants, setRestaurants] = useState<Restaurant[]>([]);

    // Filter & Search
    const [searchTerm, setSearchTerm] = useState('');
    const [filterStatus, setFilterStatus] = useState<RestaurantFilter>('All');

    // Pagination
    const [currentPage, setCurrentPage] = useState(1);

    // State
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Modal
    const [selectedRestaurant, setSelectedRestaurant] = useState<Restaurant | null>(null);
    const [showDeleteModal, setShowDeleteModal] = useState(false);
    const [showMenuModal, setShowMenuModal] = useState(false);

    /**
     * Load restaurants from API
     */
    const loadRestaurants = useCallback(async () => {
        try {
            setLoading(true);
            setError(null);
            const data = await getRestaurantsApi();
            setRestaurants(data);
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể tải nhà hàng';
            setError(message);
            console.error('Error loading restaurants:', err);
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Filter restaurants based on search and status
     */
    const filteredRestaurants = useMemo(() => {
        return restaurants.filter(r => {
            // Search filter
            const matchesSearch = searchTerm === '' ||
                r.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                r.address.toLowerCase().includes(searchTerm.toLowerCase());

            // Status filter
            const matchesStatus = filterStatus === 'All' || r.status === filterStatus;

            return matchesSearch && matchesStatus;
        });
    }, [restaurants, searchTerm, filterStatus]);

    /**
     * Pagination
     */
    const totalPages = Math.ceil(filteredRestaurants.length / ITEMS_PER_PAGE);

    const paginatedRestaurants = useMemo(() => {
        const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
        return filteredRestaurants.slice(startIndex, startIndex + ITEMS_PER_PAGE);
    }, [filteredRestaurants, currentPage]);

    // Reset page when filter changes
    useEffect(() => {
        setCurrentPage(1);
    }, [searchTerm, filterStatus]);

    /**
     * Create restaurant
     */
    const createRestaurant = useCallback(async (data: any): Promise<boolean> => {
        try {
            await createRestaurantApi(data);
            await loadRestaurants();
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể tạo nhà hàng';
            setError(message);
            console.error('Error creating restaurant:', err);
            return false;
        }
    }, [loadRestaurants]);

    /**
     * Update restaurant
     */
    const updateRestaurant = useCallback(async (id: string, data: any): Promise<boolean> => {
        try {
            await updateRestaurantApi(id, data);
            await loadRestaurants();
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể cập nhật nhà hàng';
            setError(message);
            console.error('Error updating restaurant:', err);
            return false;
        }
    }, [loadRestaurants]);

    /**
     * Delete restaurant
     */
    const deleteRestaurant = useCallback(async (id: string): Promise<boolean> => {
        try {
            await deleteRestaurantApi(id);
            await loadRestaurants();
            setShowDeleteModal(false);
            setSelectedRestaurant(null);
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể xóa nhà hàng';
            setError(message);
            console.error('Error deleting restaurant:', err);
            return false;
        }
    }, [loadRestaurants]);

    /**
     * Toggle restaurant status
     */
    const toggleStatus = useCallback(async (id: string, currentStatus: string): Promise<boolean> => {
        try {
            const newStatus = currentStatus === 'Active' ? 'Inactive' : 'Active';
            await updateRestaurantStatusApi(id, newStatus);
            await loadRestaurants();
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể thay đổi trạng thái';
            setError(message);
            console.error('Error toggling status:', err);
            return false;
        }
    }, [loadRestaurants]);

    // Load restaurants on mount
    useEffect(() => {
        loadRestaurants();
    }, [loadRestaurants]);

    return {
        restaurants,
        filteredRestaurants,
        searchTerm,
        setSearchTerm,
        filterStatus,
        setFilterStatus,
        currentPage,
        setCurrentPage,
        totalPages,
        paginatedRestaurants,
        loading,
        error,
        selectedRestaurant,
        setSelectedRestaurant,
        showDeleteModal,
        setShowDeleteModal,
        showMenuModal,
        setShowMenuModal,
        loadRestaurants,
        createRestaurant,
        updateRestaurant,
        deleteRestaurant,
        toggleStatus,
    };
};
