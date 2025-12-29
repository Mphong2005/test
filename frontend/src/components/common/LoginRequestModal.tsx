
import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Modal } from './Modal';
import { Lock } from 'lucide-react';

interface LoginRequestModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export const LoginRequestModal: React.FC<LoginRequestModalProps> = ({ isOpen, onClose }) => {
  const navigate = useNavigate();

  const handleLogin = () => {
    onClose();
    navigate('/login');
  };

  return (
    <Modal 
      isOpen={isOpen} 
      onClose={onClose} 
      maxWidth="lg" // ~512px, gần với 520px yêu cầu
      hideCloseButton={true}
      className="rounded-2xl" 
    >
      <div className="flex flex-col">
        {/* Header Section: Icon Left, Text Right */}
        <div className="flex items-start gap-4 mb-7">
          {/* Icon */}
          <div className="w-12 h-12 bg-[#fde8e0] rounded-full flex items-center justify-center shrink-0 text-[#f05a28]">
             <Lock size={22} />
          </div>

          {/* Text Content */}
          <div className="pt-0.5">
            <h3 className="text-[20px] font-semibold text-gray-900 leading-tight mb-1.5">Yêu cầu đăng nhập</h3>
            <p className="text-[14px] text-[#888] leading-relaxed">
              Bạn cần đăng nhập tài khoản để thực hiện thao tác này. Vui lòng đăng nhập để tiếp tục trải nghiệm dịch vụ.
            </p>
          </div>
        </div>

        {/* Actions Section */}
        <div className="flex w-full gap-4">
          <button 
            onClick={onClose}
            className="flex-1 h-11 rounded-full border border-[#ddd] bg-white text-[#333] font-medium text-[15px] hover:bg-gray-50 transition-all"
          >
            Trở lại
          </button>
          <button 
            onClick={handleLogin}
            className="flex-1 h-11 rounded-full bg-[#f05a28] text-white font-medium text-[15px] hover:opacity-90 transition-all shadow-sm border border-transparent"
          >
            Đăng nhập
          </button>
        </div>
      </div>
    </Modal>
  );
};
