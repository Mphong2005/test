/**
 * Utils Index
 * Re-export tất cả utilities từ một nơi
 */

// Filters
export {
  filterBySearch,
  filterByField,
  applyFilters,
} from './filters';

// Formatters
export {
  formatDateVN,
  formatDateISO,
  formatCurrency,
  formatNumber,
  formatPhone,
  stripPhoneFormat,
  getInitials,
  normalizeString,
} from './formatters';

// Mappers
export {
  ROLE_MAP,
  ROLE_MAP_REVERSE,
  GENDER_MAP,
  GENDER_MAP_REVERSE,
  STATUS_OPTIONS,
  mapRole,
  mapRoleToBackend,
  mapGender,
  mapGenderToBackend,
} from './mappers';

// Pagination
export {
  calculateTotalPages,
  paginate,
  type PaginationInfo,
  type PaginatedResult,
} from './pagination';

// Storage
export {
  STORAGE_KEYS,
  getItem,
  setItem,
  removeItem,
  removeItems,
  clearAuthData,
  getToken,
  setToken,
  getRefreshToken,
  setRefreshToken,
  getAdminInfo,
  setAdminInfo,
} from './storage';

// Validation
export {
  isValidEmail,
  isValidPhone,
  validatePassword,
  validateName,
  validateDob,
} from './validation';

// Date Helpers
export {
  parseOrderDate,
  isSameDate,
  formatDateLabelVN,
  groupByDate,
  getRelativeDateLabel,
  type DateGroup,
} from './dateHelpers';

// Error Helpers
export {
  extractErrorMessage,
  isVoucherError,
  isNetworkError,
  getErrorType,
  type ErrorType,
} from './errorHelpers';
