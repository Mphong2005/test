import React, { useState, useCallback, useEffect, useMemo } from 'react';
import { Modal } from '../common/Modal';
import { Button } from '../common/Button';
import { UserFormData } from '../../types/admin';
import {
    FormErrors,
    isValidEmail,
    isValidPhone,
    validatePassword,
    validateName,
    validateDob
} from '../../utils';
import { AlertCircle } from 'lucide-react';

interface UserFormModalProps {
    isOpen: boolean;
    onClose: () => void;
    onSave: (e: React.FormEvent<HTMLFormElement>) => void;
    mode: 'ADD' | 'EDIT';
    initialData?: {
        id?: string;
        name?: string;
        email?: string;
        phone?: string;
        gender?: string;
        dob?: string;
        role?: string;
    } | null;
    existingUsers?: { id: string; email: string; phone: string }[];
}

export const UserFormModal: React.FC<UserFormModalProps> = ({
    isOpen,
    onClose,
    onSave,
    mode,
    initialData,
    existingUsers = []
}) => {
    const [formData, setFormData] = useState<UserFormData>({
        name: initialData?.name || '',
        email: initialData?.email || '',
        phone: initialData?.phone || '',
        password: '',
        gender: initialData?.gender || 'Nam',
        dob: initialData?.dob || '',
        role: initialData?.role || 'User',
    });

    const [formErrors, setFormErrors] = useState<FormErrors>({});

    // Reset formData whenever modal opens or initialData changes
    useEffect(() => {
        setFormData({
            name: initialData?.name || '',
            email: initialData?.email || '',
            phone: initialData?.phone || '',
            password: '',
            gender: initialData?.gender || 'Nam',
            dob: initialData?.dob || '',
            role: initialData?.role || 'User',
        });
        setFormErrors({});
    }, [mode, initialData]);

    // CSS Classes
    const inputClass = "block w-full rounded-lg border bg-white text-gray-900 placeholder-gray-400 focus:border-[#EE501C] focus:ring-1 focus:ring-[#EE501C] sm:text-sm p-2.5 transition-shadow";
    const labelClass = "block text-sm font-medium text-gray-700 mb-1";
    const errorTextClass = "text-xs text-red-600 mt-1 flex items-center gap-1";

    const validateForm = useCallback((): FormErrors => {
        const errors: FormErrors = {};

        const nameResult = validateName(formData.name);
        if (!nameResult.isValid) {
            errors.name = nameResult.message;
        }

        if (!formData.email.trim()) {
            errors.email = 'Email không được để trống';
        } else if (!isValidEmail(formData.email)) {
            errors.email = 'Email không hợp lệ';
        } else if (existingUsers.some(u => u.email === formData.email && u.id !== initialData?.id)) {
            errors.email = 'Email này đã được sử dụng';
        }

        if (!formData.phone.trim()) {
            errors.phone = 'Số điện thoại không được để trống';
        } else if (!isValidPhone(formData.phone)) {
            errors.phone = 'Số điện thoại không hợp lệ (phải 10-11 chữ số)';
        } else if (existingUsers.some(u => u.phone === formData.phone && u.id !== initialData?.id)) {
            errors.phone = 'Số điện thoại này đã được sử dụng';
        }

        if (mode === 'ADD') {
            const passwordResult = validatePassword(formData.password);
            if (!passwordResult.isValid) {
                errors.password = passwordResult.message;
            }
        }

        const dobResult = validateDob(formData.dob);
        if (!dobResult.isValid) {
            errors.dob = dobResult.message;
        }

        return errors;
    }, [formData, existingUsers, initialData?.id, mode]);

    const errors = useMemo(() => validateForm(), [validateForm]);

    const handleInputChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
        const { name, value } = e.target;
        setFormData(prev => ({
            ...prev,
            [name]: value,
        }));
    };

    const handleSubmit = (e: React.FormEvent<HTMLFormElement>) => {
        const validationErrors = validateForm();
        if (Object.keys(validationErrors).length > 0) {
            setFormErrors(validationErrors);
            e.preventDefault();
            return;
        }

        Object.entries(formData).forEach(([key, value]) => {
            const input = e.currentTarget.elements.namedItem(key) as HTMLInputElement | HTMLSelectElement;
            if (input) {
                input.value = String(value);
            }
        });

        onSave(e);
    };

    return (
        <Modal
            isOpen={isOpen}
            onClose={onClose}
            title={mode === 'ADD' ? 'Thêm người dùng mới' : 'Chỉnh sửa vai trò người dùng'}
        >
            <form onSubmit={handleSubmit} className="space-y-5">
                {mode === 'ADD' ? (
                    <>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div className="space-y-1">
                                <label htmlFor="name" className={labelClass}>Họ và tên <span className="text-red-500">*</span></label>
                                <input
                                    type="text"
                                    name="name"
                                    value={formData.name}
                                    onChange={handleInputChange}
                                    className={`${inputClass} ${errors.name ? 'border-red-500' : 'border-gray-300'}`}
                                    placeholder="Nhập họ và tên"
                                />
                                {errors.name && (
                                    <div className={errorTextClass}>
                                        <AlertCircle size={14} />
                                        {errors.name}
                                    </div>
                                )}
                            </div>
                            <div className="space-y-1">
                                <label htmlFor="phone" className={labelClass}>Số điện thoại <span className="text-red-500">*</span></label>
                                <input
                                    type="tel"
                                    name="phone"
                                    value={formData.phone}
                                    onChange={handleInputChange}
                                    className={`${inputClass} ${errors.phone ? 'border-red-500' : 'border-gray-300'}`}
                                    placeholder="09xxxxxxx"
                                />
                                {errors.phone && (
                                    <div className={errorTextClass}>
                                        <AlertCircle size={14} />
                                        {errors.phone}
                                    </div>
                                )}
                            </div>
                        </div>

                        <div className="space-y-1">
                            <label htmlFor="email" className={labelClass}>Email <span className="text-red-500">*</span></label>
                            <input
                                type="email"
                                name="email"
                                value={formData.email}
                                onChange={handleInputChange}
                                className={`${inputClass} ${errors.email ? 'border-red-500' : 'border-gray-300'}`}
                                placeholder="example@email.com"
                            />
                            {errors.email && (
                                <div className={errorTextClass}>
                                    <AlertCircle size={14} />
                                    {errors.email}
                                </div>
                            )}
                        </div>

                        <div className="space-y-1">
                            <label htmlFor="password" className={labelClass}>Mật khẩu <span className="text-red-500">*</span></label>
                            <input
                                type="password"
                                name="password"
                                value={formData.password}
                                onChange={handleInputChange}
                                className={`${inputClass} ${errors.password ? 'border-red-500' : 'border-gray-300'}`}
                                placeholder="••••••••"
                            />
                            {errors.password && (
                                <div className={errorTextClass}>
                                    <AlertCircle size={14} />
                                    {errors.password}
                                </div>
                            )}
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div className="space-y-1">
                                <label htmlFor="gender" className={labelClass}>Giới tính</label>
                                <select
                                    name="gender"
                                    value={formData.gender}
                                    onChange={handleInputChange}
                                    className={`${inputClass} border-gray-300`}
                                >
                                    <option value="Nam">Nam</option>
                                    <option value="Nữ">Nữ</option>
                                    <option value="Khác">Khác</option>
                                </select>
                            </div>
                            <div className="space-y-1">
                                <label htmlFor="dob" className={labelClass}>Ngày sinh</label>
                                <input
                                    type="date"
                                    name="dob"
                                    value={formData.dob}
                                    onChange={handleInputChange}
                                    className={`${inputClass} ${errors.dob ? 'border-red-500' : 'border-gray-300'}`}
                                />
                                {errors.dob && (
                                    <div className={errorTextClass}>
                                        <AlertCircle size={14} />
                                        {errors.dob}
                                    </div>
                                )}
                            </div>
                        </div>

                        <div className="space-y-1">
                            <label htmlFor="role" className={labelClass}>Vai trò</label>
                            <select
                                name="role"
                                value={formData.role}
                                onChange={handleInputChange}
                                className={`${inputClass} border-gray-300`}
                            >
                                <option value="User">User</option>
                                <option value="Shipper">Shipper</option>
                            </select>
                        </div>
                    </>
                ) : (
                    <>
                        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
                            <p className="text-sm text-blue-800">
                                <strong>Lưu ý:</strong> Hiện tại chỉ có thể chỉnh sửa vai trò của người dùng. Các thông tin khác cần được cập nhật bởi chính người dùng đó.
                            </p>
                        </div>
                        <div className="space-y-4">
                            <div>
                                <label className={labelClass}>Họ và tên</label>
                                <input type="text" value={initialData?.name || ''} className={`${inputClass} border-gray-300`} disabled />
                            </div>
                            <div>
                                <label className={labelClass}>Email</label>
                                <input type="email" value={initialData?.email || ''} className={`${inputClass} border-gray-300`} disabled />
                            </div>
                            <div className="space-y-1">
                                <label htmlFor="role" className={labelClass}>Vai trò</label>
                                <select
                                    name="role"
                                    value={formData.role}
                                    onChange={handleInputChange}
                                    className={`${inputClass} border-gray-300`}
                                >
                                    <option value="User">User</option>
                                    <option value="Shipper">Shipper</option>
                                </select>
                            </div>
                        </div>
                    </>
                )}

                <div className="flex justify-end gap-3 pt-6 border-t border-gray-100">
                    <Button type="button" variant="secondary" onClick={onClose}>Hủy bỏ</Button>
                    <Button type="submit" disabled={mode === 'ADD' && Object.keys(errors).length > 0}>
                        {mode === 'ADD' ? 'Thêm mới' : 'Lưu thay đổi'}
                    </Button>
                </div>
            </form>
        </Modal>
    );
};
