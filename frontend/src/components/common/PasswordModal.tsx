
import React, { useState } from 'react';
import { Lock, Eye, EyeOff, Loader2, X } from 'lucide-react';

interface PasswordModalProps {
    isOpen: boolean;
    onClose: () => void;
    onConfirm: (password: string) => void | Promise<void>;
    isProcessing?: boolean;
    title?: string;
    subtitle?: string;
    confirmButtonText?: string;
    cancelButtonText?: string;
    showForgotPassword?: boolean;
    /** Optional info to display above the password input */
    infoContent?: React.ReactNode;
}

export const PasswordModal: React.FC<PasswordModalProps> = ({
    isOpen,
    onClose,
    onConfirm,
    isProcessing = false,
    title = 'Nhập mật khẩu',
    subtitle = 'Để bảo mật, vui lòng xác nhận danh tính',
    confirmButtonText = 'Xác nhận',
    cancelButtonText = 'Hủy',
    showForgotPassword = true,
    infoContent
}) => {
    const [password, setPassword] = useState('');
    const [showPasswordText, setShowPasswordText] = useState(false);
    const [passwordError, setPasswordError] = useState('');

    const handleConfirm = async () => {
        if (!password) {
            setPasswordError('Vui lòng nhập mật khẩu.');
            return;
        }
        setPasswordError('');
        await onConfirm(password);
    };

    const handleClose = () => {
        if (!isProcessing) {
            setPassword('');
            setPasswordError('');
            onClose();
        }
    };

    if (!isOpen) return null;

    return (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4">
            <div
                className="absolute inset-0 bg-[#221510]/60 backdrop-blur-sm"
                onClick={handleClose}
            />
            <div className="relative w-full max-w-[400px] bg-white rounded-3xl shadow-2xl overflow-hidden animate-in fade-in zoom-in duration-300">
                {/* Header */}
                <div className="bg-[#EE501C] p-6 pb-10 text-center relative overflow-hidden">
                    <div
                        className="absolute top-0 left-0 w-full h-full opacity-10"
                        style={{ backgroundImage: "url('https://www.transparenttextures.com/patterns/cubes.png')" }}
                    />

                    {/* Close button */}
                    <button
                        onClick={handleClose}
                        className="absolute top-4 right-4 w-8 h-8 rounded-full bg-white/20 flex items-center justify-center hover:bg-white/30 transition-colors z-20"
                    >
                        <X className="w-4 h-4 text-white" />
                    </button>

                    <div className="inline-flex p-3 rounded-full bg-white/20 backdrop-blur-md mb-4 ring-4 ring-white/10 shadow-inner">
                        <Lock className="w-8 h-8 text-white" />
                    </div>
                    <h3 className="text-xl font-bold text-white relative z-10">{title}</h3>
                    <p className="text-white/80 text-sm mt-1 relative z-10 font-medium">{subtitle}</p>
                </div>

                {/* Body */}
                <div className="px-6 py-8 -mt-6 bg-white rounded-t-3xl relative">
                    <div className="space-y-6">
                        {/* Optional info content */}
                        {infoContent && (
                            <div className="bg-orange-50 rounded-2xl p-4">
                                {infoContent}
                            </div>
                        )}

                        <div className="space-y-2">
                            <label className="text-sm font-bold text-[#1b110d] ml-1 flex items-center justify-between">
                                Mật khẩu tài khoản
                            </label>
                            <div className="relative group">
                                <input
                                    autoFocus
                                    type={showPasswordText ? "text" : "password"}
                                    value={password}
                                    disabled={isProcessing}
                                    onChange={(e) => { setPassword(e.target.value); if (passwordError) setPasswordError(''); }}
                                    className="w-full pl-4 pr-12 py-3.5 bg-[#f8f6f6] border-2 border-transparent focus:border-[#EE501C]/20 focus:bg-white focus:ring-0 rounded-xl transition-all font-bold text-[#1b110d] placeholder:text-gray-400 placeholder:font-normal text-lg"
                                    placeholder="••••••••"
                                />
                                <button
                                    type="button"
                                    onClick={() => setShowPasswordText(!showPasswordText)}
                                    className="absolute right-0 top-0 h-full px-4 text-gray-400 hover:text-[#EE501C] transition-colors focus:outline-none"
                                >
                                    {showPasswordText ? <EyeOff size={20} /> : <Eye size={20} />}
                                </button>
                            </div>

                            {passwordError && <p className="text-xs font-bold text-red-500 ml-1">{passwordError}</p>}

                            {showForgotPassword && (
                                <div className="flex justify-end pt-1">
                                    <a href="#" className="text-xs font-bold text-[#EE501C] hover:text-[#d94110] hover:underline transition-colors">Quên mật khẩu?</a>
                                </div>
                            )}
                        </div>

                        <div className="grid grid-cols-2 gap-3 pt-2">
                            <button
                                disabled={isProcessing}
                                onClick={handleClose}
                                className="py-3.5 rounded-xl font-bold text-[#9a5f4c] bg-white border-2 border-[#f3eae7] hover:bg-[#f8f6f6] hover:text-[#1b110d] hover:border-gray-300 transition-all"
                            >
                                {cancelButtonText}
                            </button>
                            <button
                                disabled={isProcessing}
                                onClick={handleConfirm}
                                className="py-3.5 rounded-xl font-bold text-white bg-[#EE501C] hover:bg-[#d94110] shadow-lg shadow-orange-200 transition-all hover:-translate-y-0.5 active:translate-y-0 flex items-center justify-center gap-2"
                            >
                                {isProcessing && <Loader2 className="w-4 h-4 animate-spin" />}
                                {confirmButtonText}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};
