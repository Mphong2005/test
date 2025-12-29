/**
 * Filter Utilities
 * Các hàm lọc và tìm kiếm dùng chung cho toàn bộ ứng dụng
 */

/**
 * Tìm kiếm trong danh sách theo nhiều trường
 * @param items - Danh sách items
 * @param searchTerm - Từ khóa tìm kiếm
 * @param fields - Các trường cần tìm
 * @returns Danh sách đã lọc
 * 
 * @example
 * // Tìm user theo name, email, phone
 * filterBySearch(users, 'john', ['name', 'email', 'phone']);
 */
export const filterBySearch = <T>(
  items: T[],
  searchTerm: string,
  fields: (keyof T)[]
): T[] => {
  if (!searchTerm || searchTerm.trim() === '') return items;

  const term = searchTerm.toLowerCase().trim();

  return items.filter(item =>
    fields.some(field => {
      const value = item[field];
      if (value === null || value === undefined) return false;
      return String(value).toLowerCase().includes(term);
    })
  );
};

/**
 * Lọc theo một trường cụ thể
 * @param items - Danh sách items
 * @param field - Trường cần lọc
 * @param value - Giá trị cần lọc (nếu 'All' hoặc undefined sẽ trả về tất cả)
 * @returns Danh sách đã lọc
 * 
 * @example
 * // Lọc user theo role
 * filterByField(users, 'role', 'Admin');
 * 
 * // Lọc đơn hàng theo status
 * filterByField(orders, 'status', 'pending');
 */
export const filterByField = <T>(
  items: T[],
  field: keyof T,
  value: unknown
): T[] => {
  if (value === 'All' || value === undefined || value === null || value === '') {
    return items;
  }
  return items.filter(item => item[field] === value);
};

/**
 * Kết hợp nhiều filter
 * @param items - Danh sách items
 * @param filters - Mảng các hàm filter
 * @returns Danh sách đã lọc qua tất cả filters
 * 
 * @example
 * const result = applyFilters(foods, [
 *   (items) => filterBySearch(items, searchTerm, ['name']),
 *   (items) => filterByField(items, 'category', category),
 * ]);
 */
export const applyFilters = <T>(
  items: T[],
  filters: ((items: T[]) => T[])[]
): T[] => {
  return filters.reduce((result, filter) => filter(result), items);
};
