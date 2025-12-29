import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';

import { AuthLayout } from '../../layouts/AuthLayout';
import { useAuthContext } from '../../contexts/AuthContext';
import { getTokenPayload } from '../../services/authService';

type LoginForm = {
  email: string;
  password: string;
};

const initialState: LoginForm = {
  email: '',
  password: '',
};

export const LoginPage: React.FC = () => {
  const navigate = useNavigate();
  const { login } = useAuthContext(); // Sử dụng login từ Context
  const [form, setForm] = useState<LoginForm>(initialState);
  const [status, setStatus] = useState<'idle' | 'loading' | 'error' | 'success'>('idle');
  const [message, setMessage] = useState<string | null>(null);

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = event.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    setStatus('loading');
    setMessage(null);

    // Gọi login từ Context để Header tự động cập nhật
    const result = await login({
      email: form.email.trim(),
      password: form.password,
    });

    if (result.success) {
      setStatus('success');
      setMessage(result.message);

      // Logic phân quyền
      // Lấy payload để check role chính xác hơn (isAdmin trong result đôi khi chỉ check admin)
      const payload = getTokenPayload();
      const role = payload?.role;

      if (role === 'admin') {
        navigate('/admin');
      } else if (role === 'shipper') {
        navigate('/shipper');
      } else {
        navigate('/');
      }
    } else {
      setStatus('error');
      setMessage(result.message);
    }
  };

  return (
    <AuthLayout
      title="Đăng nhập"
      subtitle="Truy cập tài khoản FoodDelivery của bạn"
      helper={
        <div className="space-y-3">
          <p>
            Chưa có tài khoản?{' '}
            <Link to="/register" className="font-semibold text-primary hover:text-primary/80">
              Đăng ký ngay
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
      <form className="space-y-5" onSubmit={handleSubmit}>
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
            placeholder="email@example.com"
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
            placeholder="••••••••"
            className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
          />
        </div>

        <div className="text-right">
          <Link to="/forgot-password" className="text-sm font-medium text-primary hover:text-primary/80">
            Quên mật khẩu?
          </Link>
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
          {status === 'loading' ? 'Đang xử lý...' : 'Đăng nhập'}
        </button>
      </form>
    </AuthLayout>
  );
};
