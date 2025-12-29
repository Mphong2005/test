
import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ShipperSidebar } from '../components/shipper/ShipperSidebar';
import { useAuthContext } from '../contexts/AuthContext';
import { ShipperProfile } from '../types/shipper';
import { getShipperProfileApi } from '../api/shipperApi';

interface ShipperLayoutProps {
  children: React.ReactNode;
}

export const ShipperLayout: React.FC<ShipperLayoutProps> = ({ children }) => {
  const navigate = useNavigate();
  const { logout } = useAuthContext();
  const [profile, setProfile] = useState<ShipperProfile>({
    name: '',
    email: '',
    avatar: '',
    rank: '',
    joinDate: '',
    phone: '',
    address: '',
    dob: ''
  });

  useEffect(() => {
    const fetchProfile = async () => {
      try {
        const data = await getShipperProfileApi();
        setProfile(data);
      } catch (error) {
        console.error('Failed to load shipper profile in layout', error);
      }
    };
    fetchProfile();
  }, []);

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  return (
    <div className="flex min-h-screen bg-[#FAFAFA] font-sans text-gray-900">
      <ShipperSidebar onLogout={handleLogout} profile={profile} />
      <main className="flex-1 lg:ml-72 min-h-screen p-4 md:p-8">
        <div className="max-w-[1200px] mx-auto h-full">
            {children}
        </div>
      </main>
    </div>
  );
};
