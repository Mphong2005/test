/**
 * Voucher Service
 * File này chứa các nghiệp vụ liên quan đến voucher/khuyến mãi
 * Bao gồm transform data từ backend sang frontend format
 */

import { Voucher } from '../types/common';

/**
 * Map backend voucher type to frontend format
 */
export function mapVoucherType(type: string): 'Fixed' | 'Percent' | 'FreeShip' {
    const typeMap: Record<string, 'Fixed' | 'Percent' | 'FreeShip'> = {
        'Percent': 'Percent',
        'FIXED': 'Fixed',
        'Fixed': 'Fixed',
        'FREESHIP': 'FreeShip',
        'Freeship': 'FreeShip',
        'FreeShip': 'FreeShip',
    };
    return typeMap[type] || 'Percent';
}

/**
 * Map frontend voucher type to backend format
 */
export function mapVoucherTypeToBackend(type: 'Fixed' | 'Percent' | 'FreeShip'): string {
    const typeMap = {
        'Percent': 'Percent',
        'Fixed': 'Fixed',
        'FreeShip': 'Freeship',
    };
    return typeMap[type] || 'Percent';
}

/**
 * Check if voucher is expired based on end date
 */
export function isVoucherExpired(endDate: string | Date): boolean {
    const end = typeof endDate === 'string' ? new Date(endDate) : endDate;
    return end < new Date();
}

/**
 * Map voucher status based on active flag and expiry
 */
export function mapVoucherStatus(voucher: any): 'Active' | 'Inactive' | 'Expired' {
    if (isVoucherExpired(voucher.end_date)) {
        return 'Expired';
    }
    return voucher.active ? 'Active' : 'Inactive';
}

/**
 * Generate human-readable voucher condition text
 */
export function generateVoucherCondition(voucher: any): string {
    const conditions: string[] = [];

    if (voucher.min_order_amount && voucher.min_order_amount > 0) {
        conditions.push(`Đơn tối thiểu ${voucher.min_order_amount.toLocaleString('vi-VN')}đ`);
    }

    if (voucher.restaurant_id) {
        conditions.push('Áp dụng cho nhà hàng cụ thể');
    } else {
        conditions.push('Áp dụng toàn hệ thống');
    }

    if (voucher.first_order_only) {
        conditions.push('Chỉ áp dụng cho đơn đầu tiên');
    }

    return conditions.join(', ') || 'Không có điều kiện';
}

/**
 * Extract voucher ID from various formats
 */
export function extractVoucherId(voucher: any): string {
    let voucherId = voucher._id;
    if (typeof voucherId === 'object' && voucherId.$oid) {
        voucherId = voucherId.$oid;
    } else if (typeof voucherId === 'object' && voucherId.toString) {
        voucherId = voucherId.toString();
    }
    return voucherId || voucher.promo_id || String(voucher._id);
}

/**
 * Transform backend voucher data to frontend Voucher format
 */
export function transformVoucher(voucher: any): Voucher {
    return {
        id: extractVoucherId(voucher),
        code: voucher.code,
        title: voucher.promo_name,
        description: voucher.description || '',
        type: mapVoucherType(voucher.type),
        discountValue: voucher.value,
        maxDiscount: voucher.max_discount || undefined,
        minOrderValue: voucher.min_order_amount || 0,
        startDate: voucher.start_date,
        endDate: voucher.end_date,
        status: mapVoucherStatus(voucher),
        condition: generateVoucherCondition(voucher),
        isExpired: isVoucherExpired(voucher.end_date),
        restaurantId: voucher.restaurant_id || voucher.restaurantId || undefined,
    };
}

/**
 * Transform an array of vouchers
 */
export function transformVouchers(vouchers: any[]): Voucher[] {
    return vouchers.map(voucher => transformVoucher(voucher));
}

/**
 * Transform voucher for user with eligibility info
 */
export function transformVoucherForUser(voucher: any): Voucher & { eligibleFirstOrder?: boolean } {
    return {
        ...transformVoucher(voucher),
        eligibleFirstOrder: voucher.eligible_first_order || false,
    };
}

/**
 * Transform frontend voucher data to backend format for create/update
 */
export function transformVoucherToBackend(voucherData: Partial<Voucher>): any {
    const backendData: any = {};

    if (voucherData.code !== undefined) backendData.code = voucherData.code;
    if (voucherData.title !== undefined) backendData.promo_name = voucherData.title;
    if (voucherData.description !== undefined) backendData.description = voucherData.description;
    if (voucherData.type !== undefined) backendData.type = mapVoucherTypeToBackend(voucherData.type);
    if (voucherData.discountValue !== undefined) backendData.value = voucherData.discountValue;
    if (voucherData.maxDiscount !== undefined) backendData.max_discount = voucherData.maxDiscount;
    if (voucherData.minOrderValue !== undefined) backendData.min_order_amount = voucherData.minOrderValue;
    if (voucherData.restaurantId !== undefined) backendData.restaurant_id = voucherData.restaurantId || null;
    if (voucherData.status !== undefined) backendData.active = voucherData.status === 'Active';
    if (voucherData.startDate !== undefined) backendData.start_date = voucherData.startDate;
    if (voucherData.endDate !== undefined) backendData.end_date = voucherData.endDate;

    return backendData;
}
