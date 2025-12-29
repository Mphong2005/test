/**
 * Error Helpers
 * Utility functions for error handling and message extraction
 */

/**
 * Extract user-friendly error message from error object
 * Handles nested error messages from API responses
 */
export const extractErrorMessage = (error: unknown): string => {
    let message = 'Có lỗi xảy ra. Vui lòng thử lại.';

    if (error instanceof Error) {
        message = error.message;
    } else if (typeof error === 'string') {
        message = error;
    }

    // Extract the most relevant part from nested error messages
    // Backend may return: "Lỗi DB khi tạo đơn hàng: Không thể áp dụng voucher: Voucher chỉ áp dụng cho đơn đầu tiên"
    // We want to show the voucher-specific part
    if (message.includes('Không thể áp dụng voucher:')) {
        const parts = message.split('Không thể áp dụng voucher:');
        if (parts.length > 1) {
            return parts[1].trim();
        }
    }

    return message;
};

/**
 * Check if error message is voucher-related
 */
export const isVoucherError = (errorMessage: string): boolean => {
    if (!errorMessage) return false;
    const lowerMessage = errorMessage.toLowerCase();
    return (
        lowerMessage.includes('voucher') ||
        lowerMessage.includes('ưu đãi') ||
        lowerMessage.includes('đơn đầu tiên') ||
        lowerMessage.includes('không thể áp dụng') ||
        lowerMessage.includes('promo')
    );
};

/**
 * Check if error is a network error
 */
export const isNetworkError = (error: unknown): boolean => {
    if (error instanceof Error) {
        const message = error.message.toLowerCase();
        return (
            message.includes('network') ||
            message.includes('fetch') ||
            message.includes('connection') ||
            message.includes('timeout')
        );
    }
    return false;
};

/**
 * Get error type for handling
 */
export type ErrorType = 'voucher' | 'network' | 'auth' | 'validation' | 'unknown';

export const getErrorType = (error: unknown): ErrorType => {
    const message = extractErrorMessage(error);

    if (isVoucherError(message)) return 'voucher';
    if (isNetworkError(error)) return 'network';
    if (message.toLowerCase().includes('đăng nhập') || message.toLowerCase().includes('unauthorized')) return 'auth';
    if (message.toLowerCase().includes('không hợp lệ') || message.toLowerCase().includes('thiếu')) return 'validation';

    return 'unknown';
};
