import React, { useState } from 'react';
import { Link } from 'react-router-dom';

import { AuthLayout } from '../../layouts/AuthLayout';

export const ForgotPasswordPage: React.FC = () => {
  const [email, setEmail] = useState('');
  const [status, setStatus] = useState<'idle' | 'submitted'>('idle');

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setStatus('submitted');
  };

  return (
    <AuthLayout
      title="Quên mật khẩu"
      subtitle="Nhập email để nhận hướng dẫn đặt lại mật khẩu"
      helper={
        <div className="space-y-1">
          <p>
            <Link to="/login" className="font-semibold text-primary hover:text-primary/80">
              Quay lại trang đăng nhập
            </Link>
          </p>
          <p className="text-xs text-gray-400">
            (Tính năng reset sẽ hoạt động khi backend cung cấp API tương ứng)
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
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            placeholder="email@example.com"
            className="w-full rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-gray-800 focus:border-primary focus:outline-none focus:ring-2 focus:ring-primary/20"
          />
        </div>

        {status === 'submitted' && (
          <p className="rounded-2xl bg-amber-50 px-4 py-3 text-sm text-amber-700">
            Chúng tôi đã ghi nhận yêu cầu. Vui lòng bật backend để hoàn tất quy trình hoặc liên hệ
            hỗ trợ.
          </p>
        )}

        <button
          type="submit"
          className="w-full rounded-2xl bg-primary py-3 text-base font-semibold text-white shadow-lg shadow-primary/30 transition hover:bg-[#d43f0f]"
        >
          Gửi yêu cầu đặt lại
        </button>
      </form>
    </AuthLayout>
  );
};
