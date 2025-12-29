import React from 'react';

export const Footer: React.FC = () => {
  return (
    <footer className="bg-bg-footer text-white pt-12 pb-6 mt-10">
      <div className="max-w-[1280px] mx-auto px-4 md:px-10">
        {/* Footer Head */}
        <div className="mb-6">
          <h3 className="text-2xl font-bold tracking-tight">FoodDelivery</h3>
        </div>

        <hr className="border-white/20 mb-8" />

        {/* Footer Links */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-10">
          <div className="flex flex-col gap-3">
            <h4 className="font-semibold text-lg mb-1">Company</h4>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">About FoodDelivery</a>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">Blog</a>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">Careers</a>
          </div>

          <div className="flex flex-col gap-3">
            <h4 className="font-semibold text-lg mb-1">Business</h4>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">Be a FoodDelivery Merchant</a>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">Dine With FoodDelivery</a>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">FoodDelivery for Business</a>
          </div>

          <div className="flex flex-col gap-3">
            <h4 className="font-semibold text-lg mb-1">Support</h4>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">Help Centre</a>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">FAQs</a>
            <a href="#" className="opacity-80 hover:opacity-100 transition-opacity">Contact Us</a>
          </div>
        </div>

        {/* Footer Bottom */}
        <div className="text-center text-sm opacity-80 pt-6 border-t border-white/20">
          <p>© 2025 FoodDelivery • Terms of Service • Privacy Policy</p>
        </div>
      </div>
    </footer>
  );
};
