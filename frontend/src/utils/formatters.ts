/**
 * Formatter Utils
 * Các hàm tiện ích để format dữ liệu hiển thị
 */

/**
 * Extract date components từ ISO string bằng regex
 * Không dùng Date object vì sẽ bị convert timezone
 * @param dateStr - Date string dạng ISO (2026-01-02T10:06:20.541+00:00)
 * @returns Object chứa các thành phần hoặc null
 */
const extractDateParts = (dateStr: string): { year: string; month: string; day: string; hours: string; minutes: string; seconds: string } | null => {
  // ISO format: "2026-01-02T10:06:20.541+00:00" hoặc "2026-01-02T10:06:20.541Z" hoặc "2026-01-02T10:06:20"
  const match = dateStr.match(/(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2})(?::(\d{2}))?)?/);
  if (match) {
    return {
      year: match[1],
      month: match[2],
      day: match[3],
      hours: match[4] || '00',
      minutes: match[5] || '00',
      seconds: match[6] || '00',
    };
  }
  return null;
};

/**
 * Parse date string từ backend - KHÔNG dùng Date object để tránh timezone conversion
 * @deprecated Sử dụng extractDateParts thay thế
 */
export const parseLocalDate = (dateStr: string | Date | null | undefined): Date | null => {
  if (!dateStr) return null;
  if (dateStr instanceof Date) return dateStr;

  const parts = extractDateParts(dateStr);
  if (parts) {
    // Tạo Date object từ các thành phần đã extract (sẽ được interpret là local time)
    return new Date(
      parseInt(parts.year),
      parseInt(parts.month) - 1,
      parseInt(parts.day),
      parseInt(parts.hours),
      parseInt(parts.minutes),
      parseInt(parts.seconds)
    );
  }
  return null;
};

/**
 * Format date to Vietnamese locale
 * @param date - Date string hoặc Date object
 * @returns Formatted date string (dd/mm/yyyy)
 */
export const formatDateVN = (date: string | Date | null | undefined): string => {
  if (!date) return 'Chưa cập nhật';

  try {
    if (typeof date === 'string') {
      const parts = extractDateParts(date);
      if (parts) {
        return `${parts.day}/${parts.month}/${parts.year}`;
      }
    }
    // Fallback cho Date object
    if (date instanceof Date) {
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      return `${day}/${month}/${year}`;
    }
    return 'Chưa cập nhật';
  } catch {
    return 'Chưa cập nhật';
  }
};

/**
 * Format date and time to Vietnamese locale
 * @param date - Date string hoặc Date object
 * @returns Formatted date time string (HH:mm:ss dd/mm/yyyy)
 */
export const formatDateTimeVN = (date: string | Date | null | undefined): string => {
  if (!date) return 'Chưa cập nhật';

  try {
    if (typeof date === 'string') {
      const parts = extractDateParts(date);
      if (parts) {
        return `${parts.hours}:${parts.minutes}:${parts.seconds} ${parts.day}/${parts.month}/${parts.year}`;
      }
    }
    // Fallback cho Date object
    if (date instanceof Date) {
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      const seconds = String(date.getSeconds()).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      return `${hours}:${minutes}:${seconds} ${day}/${month}/${year}`;
    }
    return 'Chưa cập nhật';
  } catch {
    return 'Chưa cập nhật';
  }
};

/**
 * Format date to ISO format (YYYY-MM-DD)
 * @param date - Date string hoặc Date object
 * @returns ISO date string
 */
export const formatDateISO = (date: string | Date | null | undefined): string => {
  if (!date) return '';

  try {
    if (typeof date === 'string') {
      const parts = extractDateParts(date);
      if (parts) {
        return `${parts.year}-${parts.month}-${parts.day}`;
      }
    }
    // Fallback cho Date object
    if (date instanceof Date) {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      return `${year}-${month}-${day}`;
    }
    return '';
  } catch {
    return '';
  }
};

/**
 * Format currency to Vietnamese Dong
 * @param amount - Amount number
 * @returns Formatted currency string
 */
export const formatCurrency = (amount: number): string => {
  return new Intl.NumberFormat('vi-VN', {
    style: 'currency',
    currency: 'VND',
  }).format(amount);
};

/**
 * Format currency without symbol
 * @param amount - Amount number
 * @returns Formatted number string with separators
 */
export const formatNumber = (amount: number): string => {
  return new Intl.NumberFormat('vi-VN').format(amount);
};

/**
 * Format phone number for display
 * @param phone - Phone number string
 * @returns Formatted phone string
 */
export const formatPhone = (phone: string | null | undefined): string => {
  if (!phone) return 'N/A';

  // Remove all non-digits
  const digits = phone.replace(/\D/g, '');

  // Format as 0xxx xxx xxx
  if (digits.length === 10) {
    return `${digits.slice(0, 4)} ${digits.slice(4, 7)} ${digits.slice(7)}`;
  }

  // Format as (+84) xxx xxx xxx
  if (digits.length === 11 && digits.startsWith('84')) {
    return `(+${digits.slice(0, 2)}) ${digits.slice(2, 5)} ${digits.slice(5, 8)} ${digits.slice(8)}`;
  }

  return phone;
};

/**
 * Strip phone number format to only digits for backend
 * @param phone - Formatted phone number string
 * @returns Phone number with only digits
 */
export const stripPhoneFormat = (phone: string | null | undefined): string => {
  if (!phone) return '';
  // Remove all non-digits
  return phone.replace(/\D/g, '');
};

/**
 * Get initials from name
 * @param name - Full name
 * @returns Initials (max 2 characters)
 */
export const getInitials = (name: string): string => {
  if (!name) return '';

  const words = name.trim().split(' ');
  if (words.length === 1) {
    return words[0].charAt(0).toUpperCase();
  }

  return (words[0].charAt(0) + words[words.length - 1].charAt(0)).toUpperCase();
};

/**
 * Normalize string for search/comparison
 * Remove accents and special characters
 * @param str - Input string
 * @returns Normalized string
 */
export const normalizeString = (str: string): string => {
  if (!str) return '';
  return str
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '') // Remove accents
    .replace(/\s+/g, ' ') // Normalize whitespace
    .trim();
};
