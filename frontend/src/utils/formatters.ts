/**
 * Formatter Utils
 * Các hàm tiện ích để format dữ liệu hiển thị
 * 
 * QUAN TRỌNG: Backend lưu thời gian UTC, frontend convert sang Vietnam time (UTC+7)
 */

// Vietnam timezone constant
const VIETNAM_TIMEZONE = 'Asia/Ho_Chi_Minh';

/**
 * Parse date string thành Date object
 * @param date - Date string hoặc Date object
 * @returns Date object hoặc null
 */
export const parseLocalDate = (date: string | Date | null | undefined): Date | null => {
  if (!date) return null;
  if (date instanceof Date) return date;

  try {
    const parsed = new Date(date);
    return isNaN(parsed.getTime()) ? null : parsed;
  } catch {
    return null;
  }
};

/**
 * Format date to Vietnamese locale (dd/mm/yyyy)
 * Converts UTC to Vietnam timezone automatically
 * @param date - Date string hoặc Date object
 * @returns Formatted date string (dd/mm/yyyy)
 */
export const formatDateVN = (date: string | Date | null | undefined): string => {
  if (!date) return 'Chưa cập nhật';

  try {
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(dateObj.getTime())) return 'Chưa cập nhật';

    // Format sang Vietnam timezone
    return dateObj.toLocaleDateString('vi-VN', {
      timeZone: VIETNAM_TIMEZONE,
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    });
  } catch {
    return 'Chưa cập nhật';
  }
};

/**
 * Format date and time to Vietnamese locale
 * Converts UTC to Vietnam timezone automatically
 * @param date - Date string hoặc Date object
 * @returns Formatted date time string (HH:mm:ss dd/mm/yyyy)
 */
export const formatDateTimeVN = (date: string | Date | null | undefined): string => {
  if (!date) return 'Chưa cập nhật';

  try {
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(dateObj.getTime())) return 'Chưa cập nhật';

    // Format sang Vietnam timezone
    const time = dateObj.toLocaleTimeString('vi-VN', {
      timeZone: VIETNAM_TIMEZONE,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    });
    const dateFormatted = dateObj.toLocaleDateString('vi-VN', {
      timeZone: VIETNAM_TIMEZONE,
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    });
    return `${time} ${dateFormatted}`;
  } catch {
    return 'Chưa cập nhật';
  }
};

/**
 * Format date to ISO format (YYYY-MM-DD)
 * Uses Vietnam timezone for date extraction
 * @param date - Date string hoặc Date object
 * @returns ISO date string
 */
export const formatDateISO = (date: string | Date | null | undefined): string => {
  if (!date) return '';

  try {
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    if (isNaN(dateObj.getTime())) return '';

    // Extract date parts in Vietnam timezone
    const options: Intl.DateTimeFormatOptions = { timeZone: VIETNAM_TIMEZONE };
    const year = dateObj.toLocaleDateString('en-CA', { ...options, year: 'numeric' }).split('/')[0] ||
      String(new Date(dateObj.toLocaleString('en-US', options)).getFullYear());
    const month = String(new Date(dateObj.toLocaleString('en-US', options)).getMonth() + 1).padStart(2, '0');
    const day = String(new Date(dateObj.toLocaleString('en-US', options)).getDate()).padStart(2, '0');

    // Simpler approach: use en-CA locale which gives YYYY-MM-DD format
    return dateObj.toLocaleDateString('en-CA', { timeZone: VIETNAM_TIMEZONE });
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
