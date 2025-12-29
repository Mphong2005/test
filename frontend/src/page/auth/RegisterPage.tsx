import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { CheckCircle, Home, LogIn } from 'lucide-react';

import { AuthLayout } from '../../layouts/AuthLayout';
import { Modal } from '../../components/common/Modal';
import { useAuthContext } from '../../contexts/AuthContext';
import { isValidPhone, validatePassword, clearAuthData } from '../../utils';

type RegisterForm = {
  fullname: string;
  email: string;
  password: string;
  phoneNumber: string;
  address: string;
  birthday: string;
  gender: string;
};

const initialState: RegisterForm = {
  fullname: '',
  email: '',
  password: '',
  phoneNumber: '',
  address: '',
  birthday: '',
  gender: '',
};

export const RegisterPage: React.FC = () => {
  const navigate = useNavigate();
  const { register } = useAuthContext(); // Sử dụng register từ Context
  const [form, setForm] = useState<RegisterForm>(initialState);
  const [status, setStatus] = useState<'idle' | 'loading' | 'error' | 'success'>('idle');
  const [message, setMessage] = useState<string | null>(null);
  const [showSuccessModal, setShowSuccessModal] = useState(false);

  const handleChange = (
    event: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>,
  ) => {
    const { name, value } = event.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    // Validate using utils
    const passwordValidation = validatePassword(form.password);
    if (!passwordValidation.isValid) {
      setStatus('error');
      setMessage(passwordValidation.message || 'Mật khẩu không hợp lệ');
      return;
    }

    if (form.phoneNumber && !isValidPhone(form.phoneNumber)) {
      setStatus('error');
      setMessage('Số điện thoại phải gồm 10-11 chữ số.');
      return;
    }

    setStatus('loading');
    setMessage(null);

    // Gọi register từ Context
    const result = await register({
      fullname: form.fullname.trim(),
      email: form.email.trim(),
      password: form.password,
      phone_number: form.phoneNumber ? form.phoneNumber.trim() : undefined,
      address: form.address ? form.address.trim() : undefined,
      birthday: form.birthday || undefined,
      gender: form.gender ? (form.gender as 'Male' | 'Female') : undefined,
    });

    if (result.success) {
      setStatus('success');
      setMessage(result.message);
      setForm(initialState);
      setShowSuccessModal(true);
    } else {
      setStatus('error');
      setMessage(result.message);
    }
  };

  return (
    <AuthLayout
      title="Đăng ký"
      subtitle="Tạo tài khoản FoodDelivery trong vài bước đơn giản"
      helper={
        <div className="space-y-3">
          <p>
            Đã có tài khoản?{' '}
            <Link to="/login" className="font-semibold text-primary hover:text-primary/80">
              Đăng nhập
            </Link>
          </p>
          <p>
            <Link to="/" className="text-sm font-medium text-gray-600 hover:text-primary">
              ← Quay lại trang chủ
            </Link>
          </p>
        </div>
      }
    >
      <form className="space-y-4" onSubmit={handleSubmit}>
        <div className="space-y-2">
          <label htmlFor="fullname" className="text-sm font-semibold text-gray-600">
            Họ và tên
          </label>
          <input
            id="fullname"
            name="fullname"
            type="text"
            required
            value={form.fullname}
            onChange={handleChange}
            //placeholder="Nguyễn Văn A"
            className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
          />
        </div>

        <div className="space-y-2">
          <label htmlFor="email" className="text-sm font-semibold text-gray-600">
            Email
          </label>
          <input
            id="email"
            name="email"
            type="email"
            required
            value={form.email}
            onChange={handleChange}
            //placeholder="email@example.com"
            className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
          />
        </div>

        <div className="space-y-2">
          <label htmlFor="password" className="text-sm font-semibold text-gray-600">
            Mật khẩu
          </label>
          <input
            id="password"
            name="password"
            type="password"
            required
            value={form.password}
            onChange={handleChange}
            placeholder="Yêu cầu ít nhất 6 ký tự"
            className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
          />
        </div>

        <div className="space-y-2">
          <label htmlFor="phoneNumber" className="text-sm font-semibold text-gray-600">
            Số điện thoại (tùy chọn)
          </label>
          <input
            id="phoneNumber"
            name="phoneNumber"
            type="tel"
            pattern="\d{10,11}"
            value={form.phoneNumber}
            onChange={handleChange}
            //placeholder="0912345678"
            className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
          />
        </div>

        <div className="space-y-2">
          <label htmlFor="address" className="text-sm font-semibold text-gray-600">
            Địa chỉ (tùy chọn)
          </label>
          <input
            id="address"
            name="address"
            type="text"
            value={form.address}
            onChange={handleChange}
            placeholder="Nhập địa chỉ của bạn"
            className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
          />
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <label htmlFor="birthday" className="text-sm font-semibold text-gray-600">
              Ngày sinh (tùy chọn)
            </label>
            <input
              id="birthday"
              name="birthday"
              type="date"
              value={form.birthday}
              onChange={handleChange}
              className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
            />
          </div>
          <div className="space-y-2">
            <label htmlFor="gender" className="text-sm font-semibold text-gray-600">
              Giới tính (tùy chọn)
            </label>
            <select
              id="gender"
              name="gender"
              value={form.gender}
              onChange={handleChange}
              className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
            >
              <option value="">Chọn giới tính</option>
              <option value="Male">Nam</option>
              <option value="Female">Nữ</option>
            </select>
          </div>
        </div>

        {message && (
          <p
            className={`rounded-2xl px-4 py-3 text-sm ${status === 'success' ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-600'
              }`}
          >
            {message}
          </p>
        )}

        <button
          type="submit"
          disabled={status === 'loading'}
          className="w-full rounded-2xl bg-primary py-3 text-base font-semibold text-white shadow-lg shadow-primary/30 transition hover:bg-[#d43f0f] disabled:cursor-not-allowed disabled:opacity-60"
        >
          {status === 'loading' ? 'Đang xử lý...' : 'Đăng ký'}
        </button>
      </form>

      {/* Success Modal */}
      <Modal
        isOpen={showSuccessModal}
        onClose={() => setShowSuccessModal(false)}
        maxWidth="sm"
        hideCloseButton
      >
        <div className="text-center py-4">
          <div className="flex justify-center mb-4">
            <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center">
              <CheckCircle className="w-10 h-10 text-green-500" />
            </div>
          </div>
          <h3 className="text-xl font-bold text-gray-800 mb-2">Đăng ký thành công!</h3>
          <p className="text-gray-600 mb-6">
            Tài khoản của bạn đã được tạo. Bạn có thể đăng nhập ngay bây giờ.
          </p>
          <div className="flex gap-3">
            <button
              onClick={() => navigate('/')}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-3 rounded-xl border-2 border-gray-200 text-gray-700 font-semibold hover:bg-gray-50 transition-colors"
            >
              <Home className="w-5 h-5" />
              Trang chủ
            </button>
            <button
              onClick={() => {
                clearAuthData();
                navigate('/login');
              }}
              className="flex-1 flex items-center justify-center gap-2 px-4 py-3 rounded-xl bg-primary text-white font-semibold hover:bg-[#d43f0f] transition-colors shadow-lg shadow-primary/30"
            >
              <LogIn className="w-5 h-5" />
              Đăng nhập
            </button>
          </div>
        </div>
      </Modal>
    </AuthLayout>
  );
};
