/**
 * Validation Utils
 * Các hàm tiện ích để validate dữ liệu
 */

// ============ Field Validators ============

/**
 * Validate email format
 * @param email - Email string
 * @returns boolean
 */
export const isValidEmail = (email: string): boolean => {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
};

/**
 * Validate phone number (Vietnamese format)
 * @param phone - Phone number string
 * @returns boolean
 */
export const isValidPhone = (phone: string): boolean => {
  const phoneRegex = /^(?:\+84|0)[0-9]{9,10}$/;
  return phoneRegex.test(phone.replace(/\s/g, ''));
};

/**
 * Validate password strength
 * @param password - Password string
 * @returns Object with isValid and message
 */
export const validatePassword = (password: string): { isValid: boolean; message?: string } => {
  if (!password) {
    return { isValid: false, message: 'Mật khẩu không được để trống' };
  }
  if (password.length < 6) {
    return { isValid: false, message: 'Mật khẩu phải có ít nhất 6 ký tự' };
  }
  return { isValid: true };
};

/**
 * Validate name
 * @param name - Name string
 * @returns Object with isValid and message
 */
export const validateName = (name: string): { isValid: boolean; message?: string } => {
  if (!name.trim()) {
    return { isValid: false, message: 'Họ và tên không được để trống' };
  }
  if (name.trim().length < 2) {
    return { isValid: false, message: 'Họ và tên phải có ít nhất 2 ký tự' };
  }
  return { isValid: true };
};

/**
 * Validate date of birth (minimum age)
 * @param dob - Date of birth string (YYYY-MM-DD)
 * @param minAge - Minimum age required (default: 13)
 * @returns Object with isValid and message
 */
export const validateDob = (dob: string, minAge: number = 13): { isValid: boolean; message?: string } => {
  if (!dob) {
    return { isValid: true }; // Optional field
  }

  const dobDate = new Date(dob);
  const today = new Date();
  let age = today.getFullYear() - dobDate.getFullYear();
  const monthDiff = today.getMonth() - dobDate.getMonth();

  if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < dobDate.getDate())) {
    age--;
  }

  if (age < minAge) {
    return { isValid: false, message: `Người dùng phải từ ${minAge} tuổi trở lên` };
  }

  return { isValid: true };
};