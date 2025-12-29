
import React, { useState, useEffect } from 'react';
import { ShipperProfile } from '../../types/shipper';
import { updateShipperProfileApi } from '../../api/shipperApi';
import { apiClient, getBackendBaseUrl } from '../../api/axiosClient';
import { Camera, Calendar, Edit2, User } from 'lucide-react';

export const ShipperProfilePage: React.FC = () => {
  const [formData, setFormData] = useState<ShipperProfile | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [gender, setGender] = useState<string>('Nam');

  useEffect(() => {
    const fetchProfile = async () => {
      try {
        setLoading(true);
        // Fetch profile data - getShipperProfileApi already calls profile_me, 
        // so we'll fetch directly to also get gender
        const response = await apiClient.get(`${getBackendBaseUrl()}/api/users/profile_me`);
        if (response.data.success && response.data.data) {
          const user = response.data.data;
          
          // Map backend gender (Male/Female) to Vietnamese (Nam/Nữ)
          if (user.gender === 'Male') setGender('Nam');
          else if (user.gender === 'Female') setGender('Nữ');
          
          // Format profile data using same logic as getShipperProfileApi
          let joinDate = '';
          const createdAt = user.created_at || user.createdAt;
          if (createdAt) {
            try {
              const date = new Date(createdAt);
              const day = String(date.getDate()).padStart(2, '0');
              const month = String(date.getMonth() + 1).padStart(2, '0');
              const year = date.getFullYear();
              joinDate = `${day}/${month}/${year}`;
            } catch {
              joinDate = createdAt;
            }
          }
          
          let dob = '';
          const birthday = user.birthday || user.dob;
          if (birthday) {
            try {
              const date = new Date(birthday);
              const year = date.getFullYear();
              const month = String(date.getMonth() + 1).padStart(2, '0');
              const day = String(date.getDate()).padStart(2, '0');
              dob = `${year}-${month}-${day}`;
            } catch {
              dob = birthday;
            }
          }
          
          setFormData({
            name: user.fullname || 'Shipper',
            email: user.email || '',
            avatar: user.avatar || 'https://images.unsplash.com/photo-1633332755192-727a05c4013d?w=400&q=60',
            rank: 'Tài xế 5 sao',
            joinDate: joinDate,
            phone: user.phone_number || '',
            address: user.address || 'Hà Nội',
            dob: dob,
          });
        }
      } catch (error) {
        console.error("Failed to fetch profile", error);
        setError('Không thể tải thông tin profile');
      } finally {
        setLoading(false);
      }
    };
    fetchProfile();
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData) return;

    try {
      setSaving(true);
      setError(null);
      const updatedProfile = await updateShipperProfileApi({
        name: formData.name,
        email: formData.email,
        phone: formData.phone,
        address: formData.address,
        dob: formData.dob,
        gender: gender,
      });
      setFormData(updatedProfile);
      alert('Thông tin cá nhân đã được lưu thành công!');
    } catch (err: any) {
      console.error('Error updating profile:', err);
      setError(err.message || 'Không thể cập nhật thông tin. Vui lòng thử lại.');
    } finally {
      setSaving(false);
    }
  };

  if (loading || !formData) {
    return <div className="flex justify-center py-20"><div className="animate-spin rounded-full h-10 w-10 border-b-2 border-[#EE501C]"></div></div>;
  }

  // Check if data exists for display
  const hasData = formData.name !== '';

  return (
    <div className="flex flex-col gap-8 animate-in fade-in duration-500 pb-20">
      <div className="shrink-0">
        <h2 className="text-2xl font-black text-gray-900">Thông tin cá nhân</h2>
        <p className="text-gray-400 text-sm mt-1 font-medium">Quản lý và cập nhật thông tin hồ sơ của bạn</p>
      </div>
      
      {hasData ? (
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-8 items-start">
        {/* Left Column - Avatar & Badges */}
        <div className="lg:col-span-4 flex flex-col items-center p-8 bg-white rounded-[3rem] border border-gray-100 shadow-sm">
          <div className="relative group cursor-pointer mb-6">
            <div 
              className="w-48 h-48 rounded-full bg-gradient-to-br from-[#EE501C] to-[#FF7043] border-4 border-white shadow-xl flex items-center justify-center text-white text-6xl font-black group-hover:opacity-90 transition-opacity" 
            >
              {formData.name ? formData.name.charAt(0).toUpperCase() : 'S'}
            </div>
            <div className="absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none rounded-full bg-black/20">
               <Camera className="text-white drop-shadow-md" size={32} />
            </div>
          </div>
          
          <h3 className="text-2xl font-black text-gray-900 tracking-tight text-center">{formData.name}</h3>
          
          <div className="flex flex-col items-center gap-2 mt-3">
            <span className="px-4 py-1.5 rounded-full bg-[#EE501C]/10 text-[#EE501C] text-xs font-bold border border-[#EE501C]/20 uppercase tracking-wide">
              {formData.rank}
            </span>
          </div>
          
          <div className="mt-8 w-full space-y-4">
             <div className="flex items-center justify-between p-5 rounded-[1.5rem] bg-gray-50 border border-gray-100">
               <div className="flex items-center gap-3">
                  <Calendar size={20} className="text-[#EE501C]" />
                  <span className="text-sm font-medium text-gray-500">Thành viên từ</span>
               </div>
               <span className="text-sm font-bold text-gray-900">{formData.joinDate}</span>
             </div>
          </div>
        </div>

        {/* Right Column - Edit Form */}
        <div className="lg:col-span-8 p-8 md:p-10 bg-white rounded-[3rem] border border-gray-100 shadow-sm">
          <form className="space-y-8" onSubmit={handleSubmit}>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              <div className="flex flex-col gap-2">
                <label className="text-xs font-bold text-gray-400 uppercase tracking-widest pl-1">Họ và tên</label>
                <div className="relative group">
                    <input 
                    className="w-full px-6 py-4 rounded-[1.5rem] border border-gray-100 bg-gray-50 text-gray-900 focus:ring-2 focus:ring-orange-100 focus:border-orange-200 outline-none transition-all font-medium placeholder:text-gray-300" 
                    type="text" 
                    value={formData.name}
                    onChange={(e) => setFormData({...formData, name: e.target.value})}
                    />
                    <Edit2 size={16} className="absolute right-6 top-1/2 -translate-y-1/2 text-gray-300 group-hover:text-[#EE501C] transition-colors pointer-events-none" />
                </div>
              </div>
              <div className="flex flex-col gap-2">
                <label className="text-xs font-bold text-gray-400 uppercase tracking-widest pl-1">Email</label>
                <div className="relative group">
                    <input 
                    className="w-full px-6 py-4 rounded-[1.5rem] border border-gray-100 bg-gray-50 text-gray-900 focus:ring-2 focus:ring-orange-100 focus:border-orange-200 outline-none transition-all font-medium placeholder:text-gray-300" 
                    type="email" 
                    value={formData.email}
                    onChange={(e) => setFormData({...formData, email: e.target.value})}
                    />
                    <Edit2 size={16} className="absolute right-6 top-1/2 -translate-y-1/2 text-gray-300 group-hover:text-[#EE501C] transition-colors pointer-events-none" />
                </div>
              </div>
              <div className="flex flex-col gap-2">
                <label className="text-xs font-bold text-gray-400 uppercase tracking-widest pl-1">Số điện thoại</label>
                <div className="relative group">
                    <input 
                    className="w-full px-6 py-4 rounded-[1.5rem] border border-gray-100 bg-gray-50 text-gray-900 focus:ring-2 focus:ring-orange-100 focus:border-orange-200 outline-none transition-all font-medium font-mono placeholder:text-gray-300" 
                    type="tel" 
                    value={formData.phone}
                    onChange={(e) => setFormData({...formData, phone: e.target.value})}
                    />
                    <Edit2 size={16} className="absolute right-6 top-1/2 -translate-y-1/2 text-gray-300 group-hover:text-[#EE501C] transition-colors pointer-events-none" />
                </div>
              </div>
              <div className="flex flex-col gap-2">
                <label className="text-xs font-bold text-gray-400 uppercase tracking-widest pl-1">Ngày sinh</label>
                <div className="relative group">
                    <input 
                    className="w-full px-6 py-4 rounded-[1.5rem] border border-gray-100 bg-gray-50 text-gray-900 focus:ring-2 focus:ring-orange-100 focus:border-orange-200 outline-none transition-all font-medium placeholder:text-gray-300" 
                    type="date" 
                    value={formData.dob}
                    onChange={(e) => setFormData({...formData, dob: e.target.value})}
                    />
                </div>
              </div>
              
              <div className="flex flex-col gap-2 md:col-span-2">
                <label className="text-xs font-bold text-gray-400 uppercase tracking-widest pl-1">Địa chỉ thường trú</label>
                <div className="relative group">
                  <input 
                    className="w-full px-6 py-4 rounded-[1.5rem] border border-gray-100 bg-gray-50 text-gray-900 focus:ring-2 focus:ring-orange-100 focus:border-orange-200 outline-none transition-all font-medium pr-12 placeholder:text-gray-300" 
                    type="text" 
                    value={formData.address}
                    onChange={(e) => setFormData({...formData, address: e.target.value})}
                  />
                  <Edit2 size={16} className="absolute right-6 top-1/2 -translate-y-1/2 text-gray-300 group-hover:text-[#EE501C] transition-colors pointer-events-none" />
                </div>
              </div>
              
              <div className="flex flex-col gap-2 md:col-span-2">
                <label className="text-xs font-bold text-gray-400 uppercase tracking-widest pl-1">Giới tính</label>
                <div className="relative group w-full md:w-1/2">
                    <select
                        className="w-full px-6 py-4 rounded-[1.5rem] border border-gray-100 bg-gray-50 text-gray-900 focus:ring-2 focus:ring-orange-100 focus:border-orange-200 outline-none transition-all font-medium appearance-none"
                        value={gender}
                        onChange={(e) => setGender(e.target.value)}
                    >
                        <option value="Nam">Nam</option>
                        <option value="Nữ">Nữ</option>
                    </select>
                    <div className="absolute right-6 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">▼</div>
                </div>
              </div>
            </div>
            
            {error && (
              <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-[1.5rem] text-red-700 text-sm">
                {error}
              </div>
            )}
            
            <div className="border-t border-gray-100 pt-8 flex items-center justify-end gap-4 mt-8">
              <button 
                className="px-10 py-4 rounded-[1.2rem] border border-gray-200 font-bold text-sm text-gray-600 hover:bg-gray-50 transition-all shadow-sm active:scale-95 disabled:opacity-50" 
                type="button"
                onClick={() => {
                  // Reload profile to reset changes
                  window.location.reload();
                }}
                disabled={saving}
              >
                Hủy
              </button>
              <button 
                className="px-12 py-4 rounded-[1.2rem] bg-[#EE501C] text-white font-bold text-sm shadow-xl shadow-orange-200 hover:bg-[#d64517] transition-all active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2" 
                type="submit"
                disabled={saving}
              >
                {saving ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    Đang lưu...
                  </>
                ) : (
                  'Lưu thay đổi'
                )}
              </button>
            </div>
          </form>
        </div>
      </div>
      ) : (
        <div className="flex flex-col items-center justify-center py-20 bg-white rounded-[3rem] border border-dashed border-gray-200">
            <div className="w-20 h-20 rounded-full bg-gray-100 flex items-center justify-center mb-4">
                <User size={40} className="text-gray-300" />
            </div>
            <h3 className="text-lg font-bold text-gray-900">Không tìm thấy thông tin</h3>
            <p className="text-gray-500">Vui lòng đăng nhập với tài khoản Shipper để xem thông tin.</p>
        </div>
      )}
    </div>
  );
};
