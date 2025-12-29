/**
 * Pagination Utilities
 * Các hàm phân trang dùng chung cho toàn bộ ứng dụng
 */

/**
 * Thông tin phân trang
 */
export interface PaginationInfo {
  currentPage: number;
  totalPages: number;
  totalItems: number;
  itemsPerPage: number;
  hasNextPage: boolean;
  hasPrevPage: boolean;
  startIndex: number;
  endIndex: number;
}

/**
 * Kết quả phân trang
 */
export interface PaginatedResult<T> {
  data: T[];
  pagination: PaginationInfo;
}

/**
 * Tính số trang tổng cộng
 * @param totalItems - Tổng số items
 * @param itemsPerPage - Số items mỗi trang
 * @returns Tổng số trang
 */
export const calculateTotalPages = (totalItems: number, itemsPerPage: number): number => {
  if (itemsPerPage <= 0) return 0;
  return Math.ceil(totalItems / itemsPerPage);
};

/**
 * Phân trang danh sách với đầy đủ thông tin
 * @param items - Danh sách items
 * @param page - Số trang hiện tại (bắt đầu từ 1)
 * @param itemsPerPage - Số items mỗi trang
 * @returns Kết quả phân trang với data và thông tin pagination
 * 
 * @example
 * const { data, pagination } = paginate(users, 1, 10);
 * // data: users của trang 1
 * // pagination: { currentPage: 1, totalPages: 5, hasNextPage: true, ... }
 */
export const paginate = <T>(
  items: T[],
  page: number,
  itemsPerPage: number
): PaginatedResult<T> => {
  const totalItems = items.length;
  const totalPages = calculateTotalPages(totalItems, itemsPerPage);

  // Đảm bảo page nằm trong khoảng hợp lệ
  const currentPage = Math.max(1, Math.min(page, totalPages || 1));

  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = Math.min(startIndex + itemsPerPage, totalItems);

  const data = items.slice(startIndex, endIndex);

  return {
    data,
    pagination: {
      currentPage,
      totalPages,
      totalItems,
      itemsPerPage,
      hasNextPage: currentPage < totalPages,
      hasPrevPage: currentPage > 1,
      startIndex: totalItems > 0 ? startIndex + 1 : 0, // 1-indexed for display
      endIndex,
    },
  };
};
