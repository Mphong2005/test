/**
 * useVoucher Hook
 * Hook xử lý logic quản lý voucher cho Admin
 * Tách từ VoucherManagement.tsx
 */

import { useState, useEffect, useCallback, useMemo } from 'react';
import { Voucher } from '../types/common';
import {
    getVouchersApi,
    createVoucherApi,
    updateVoucherApi,
    deleteVoucherApi,
} from '../api/voucherApi';

type VoucherFilter = 'All' | 'Active' | 'Inactive' | 'Expired';

interface UseVoucherReturn {
    // Data
    vouchers: Voucher[];
    filteredVouchers: Voucher[];

    // Filter & Search
    searchTerm: string;
    setSearchTerm: (term: string) => void;
    filterStatus: VoucherFilter;
    setFilterStatus: (status: VoucherFilter) => void;

    // Pagination
    currentPage: number;
    setCurrentPage: (page: number) => void;
    totalPages: number;
    paginatedVouchers: Voucher[];

    // State
    loading: boolean;
    error: string | null;

    // Modal
    selectedVoucher: Voucher | null;
    setSelectedVoucher: (voucher: Voucher | null) => void;
    showDeleteModal: boolean;
    setShowDeleteModal: (show: boolean) => void;
    showEditModal: boolean;
    setShowEditModal: (show: boolean) => void;

    // Actions
    loadVouchers: () => Promise<void>;
    createVoucher: (data: Partial<Voucher>) => Promise<boolean>;
    updateVoucher: (id: string, data: Partial<Voucher>) => Promise<boolean>;
    deleteVoucher: (id: string) => Promise<boolean>;
    toggleStatus: (voucher: Voucher) => Promise<boolean>;
}

const ITEMS_PER_PAGE = 7;

export const useVoucher = (): UseVoucherReturn => {
    // Data
    const [vouchers, setVouchers] = useState<Voucher[]>([]);

    // Filter & Search
    const [searchTerm, setSearchTerm] = useState('');
    const [filterStatus, setFilterStatus] = useState<VoucherFilter>('All');

    // Pagination
    const [currentPage, setCurrentPage] = useState(1);

    // State
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Modal
    const [selectedVoucher, setSelectedVoucher] = useState<Voucher | null>(null);
    const [showDeleteModal, setShowDeleteModal] = useState(false);
    const [showEditModal, setShowEditModal] = useState(false);

    /**
     * Load vouchers from API
     */
    const loadVouchers = useCallback(async () => {
        try {
            setLoading(true);
            setError(null);
            const data = await getVouchersApi();
            setVouchers(data);
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể tải vouchers';
            setError(message);
            console.error('Error loading vouchers:', err);
        } finally {
            setLoading(false);
        }
    }, []);

    /**
     * Filter vouchers based on search and status
     */
    const filteredVouchers = useMemo(() => {
        return vouchers.filter(v => {
            // Search filter
            const matchesSearch = searchTerm === '' ||
                v.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
                v.code.toLowerCase().includes(searchTerm.toLowerCase());

            // Status filter
            const matchesStatus = filterStatus === 'All' || v.status === filterStatus;

            return matchesSearch && matchesStatus;
        });
    }, [vouchers, searchTerm, filterStatus]);

    /**
     * Pagination
     */
    const totalPages = Math.ceil(filteredVouchers.length / ITEMS_PER_PAGE);

    const paginatedVouchers = useMemo(() => {
        const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
        return filteredVouchers.slice(startIndex, startIndex + ITEMS_PER_PAGE);
    }, [filteredVouchers, currentPage]);

    // Reset page when filter changes
    useEffect(() => {
        setCurrentPage(1);
    }, [searchTerm, filterStatus]);

    /**
     * Create voucher
     */
    const createVoucher = useCallback(async (data: Partial<Voucher>): Promise<boolean> => {
        try {
            await createVoucherApi(data as Omit<Voucher, 'id'>);
            await loadVouchers();
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể tạo voucher';
            setError(message);
            console.error('Error creating voucher:', err);
            return false;
        }
    }, [loadVouchers]);

    /**
     * Update voucher
     */
    const updateVoucher = useCallback(async (id: string, data: Partial<Voucher>): Promise<boolean> => {
        try {
            await updateVoucherApi(id, data);
            await loadVouchers();
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể cập nhật voucher';
            setError(message);
            console.error('Error updating voucher:', err);
            return false;
        }
    }, [loadVouchers]);

    /**
     * Delete voucher
     */
    const deleteVoucher = useCallback(async (id: string): Promise<boolean> => {
        try {
            await deleteVoucherApi(id);
            await loadVouchers();
            setShowDeleteModal(false);
            setSelectedVoucher(null);
            return true;
        } catch (err) {
            const message = err instanceof Error ? err.message : 'Không thể xóa voucher';
            setError(message);
            console.error('Error deleting voucher:', err);
            return false;
        }
    }, [loadVouchers]);

    /**
     * Toggle voucher status
     */
    const toggleStatus = useCallback(async (voucher: Voucher): Promise<boolean> => {
        const newStatus = voucher.status === 'Active' ? 'Inactive' : 'Active';
        return updateVoucher(voucher.id, { status: newStatus });
    }, [updateVoucher]);

    // Load vouchers on mount
    useEffect(() => {
        loadVouchers();
    }, [loadVouchers]);

    return {
        vouchers,
        filteredVouchers,
        searchTerm,
        setSearchTerm,
        filterStatus,
        setFilterStatus,
        currentPage,
        setCurrentPage,
        totalPages,
        paginatedVouchers,
        loading,
        error,
        selectedVoucher,
        setSelectedVoucher,
        showDeleteModal,
        setShowDeleteModal,
        showEditModal,
        setShowEditModal,
        loadVouchers,
        createVoucher,
        updateVoucher,
        deleteVoucher,
        toggleStatus,
    };
};
