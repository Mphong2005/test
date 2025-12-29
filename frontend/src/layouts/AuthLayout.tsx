import React from 'react';

type AuthLayoutProps = {
  title: string;
  subtitle?: string;
  helper?: React.ReactNode;
  children: React.ReactNode;
};

export const AuthLayout: React.FC<AuthLayoutProps> = ({ title, subtitle, helper, children }) => {
  return (
    <section className="relative min-h-[calc(100vh-80px)] flex items-center justify-center bg-gradient-to-b from-primary-light/40 via-white to-white px-4 py-12">
      <div className="w-full max-w-xl bg-white/95 backdrop-blur rounded-[32px] shadow-xl border border-white/60 p-8 md:p-12">
        <div className="text-center mb-8">
          <p className="text-sm uppercase tracking-[0.3em] text-primary/70 font-semibold mb-2">
            FoodDelivery
          </p>
          <h1 className="text-3xl md:text-4xl font-extrabold text-gray-900">{title}</h1>
          {subtitle && <p className="mt-3 text-gray-500">{subtitle}</p>}
        </div>

        {children}

        {helper && <div className="mt-8 text-center text-sm text-gray-600">{helper}</div>}
      </div>

      <div className="pointer-events-none absolute inset-0 flex justify-center">
        <div className="w-72 h-72 bg-primary/5 rounded-full blur-3xl" />
      </div>
    </section>
  );
};

