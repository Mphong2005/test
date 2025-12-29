
import React, { useEffect, useState } from 'react';
import { AdminLayout } from '../../layouts/AdminLayout';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell
} from 'recharts';
import {
  Download, DollarSign, Wallet, Users, Store,
  Check, X, Bike
} from 'lucide-react';
import { getDashboardStatsApi } from '../../api/dashboardApi';
import { formatNumber, extractErrorMessage, getInitials } from '../../utils';

// Format helpers specifically for the Dashboard Cards
const formatCompactMoney = (amount: number) => {
  if (amount >= 1000000000) {
    return `${(amount / 1000000000).toFixed(1).replace('.0', '')} tỷ đ`;
  }
  if (amount >= 1000000) {
    return `${(amount / 1000000).toFixed(1).replace('.0', '')}tr đ`;
  }
  return `${formatNumber(amount)} đ`;
};

// Helper Component for Stat Card with Exact Styling from Screenshot
const StatCard = ({ title, value, icon: Icon, iconBg, iconColor }: any) => (
  <div className="bg-white p-6 rounded-2xl shadow-sm border border-gray-200 flex items-center justify-between h-full hover:shadow-md transition-all">
    <div>
      <h3 className="text-sm font-bold text-gray-500 mb-1">{title}</h3>
      <div className="text-2xl font-black text-gray-900">{value}</div>
    </div>
    <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${iconBg} ${iconColor}`}>
      <Icon size={24} />
    </div>
  </div>
);

export const DashboardPage: React.FC = () => {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  // Default to current year
  const currentYear = new Date().getFullYear();
  const [year, setYear] = useState(currentYear);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        // Pass selected year to API
        const stats = await getDashboardStatsApi(year);
        console.log('Dashboard data received:', stats);
        setData(stats);
      } catch (error: unknown) {
        console.error("Failed to load dashboard data", error);
        alert(extractErrorMessage(error));
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [year]); // Re-fetch when year changes

  if (loading && !data) {
    return (
      <AdminLayout title="Dashboard">
        <div className="flex items-center justify-center h-[80vh]">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#EE501C]"></div>
        </div>
      </AdminLayout>
    );
  }

  if (!data) {
    return (
      <AdminLayout title="Dashboard">
        <div className="flex items-center justify-center h-[80vh]">
          <div className="text-center">
            <p className="text-gray-500">Không có dữ liệu để hiển thị</p>
          </div>
        </div>
      </AdminLayout>
    );
  }

  const { revenue = [], status = [], activities = [], topItems = [], topRestaurants = [], summary = {} } = data;

  // Ensure summary has default values
  const safeSummary = {
    totalRevenue: summary.totalRevenue || 0,
    todayRevenue: summary.todayRevenue || 0,
    totalUsers: summary.totalUsers || 0,
    totalRestaurants: summary.totalRestaurants || 0,
  };

  return (
    <AdminLayout title="Dashboard">
      <div className="space-y-8 animate-in fade-in duration-500 pb-10">

        {/* Dashboard Title & Actions */}
        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-4 mb-2">
          <div>
            <h2 className="text-xl font-bold text-gray-900">Tổng quan hoạt động</h2>
            <p className="text-sm text-gray-500 mt-1">Theo dõi các chỉ số quan trọng trong hệ thống.</p>
          </div>
          <button className="bg-[#EE501C] hover:bg-[#d43f0f] text-white px-5 py-2.5 rounded-lg text-sm font-bold flex items-center shadow-md shadow-orange-100 transition-all hover:-translate-y-0.5">
            <Download size={18} className="mr-2" />
            Xuất báo cáo
          </button>
        </div>

        {/* Stats Grid - Data from API */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <StatCard
            title="Tổng doanh thu"
            value={formatCompactMoney(safeSummary.totalRevenue)}
            icon={Wallet}
            iconBg="bg-red-50"
            iconColor="text-[#EE501C]"
          />
          <StatCard
            title="Doanh thu hôm nay"
            value={formatCompactMoney(safeSummary.todayRevenue)}
            icon={DollarSign}
            iconBg="bg-green-50"
            iconColor="text-emerald-500"
          />
          <StatCard
            title="Người dùng active"
            value={formatNumber(safeSummary.totalUsers)}
            icon={Users}
            iconBg="bg-blue-50"
            iconColor="text-blue-500"
          />
          <StatCard
            title="Tổng số nhà hàng"
            value={formatNumber(safeSummary.totalRestaurants)}
            icon={Store}
            iconBg="bg-orange-50"
            iconColor="text-amber-500"
          />
        </div>

        {/* Charts Row */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Bar Chart */}
          <div className="lg:col-span-2 bg-white p-6 rounded-2xl shadow-sm border border-gray-200">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-lg font-bold text-gray-900">Biểu đồ doanh thu năm {year}</h3>
              <div className="bg-gray-50 rounded-lg px-2 py-1">
                <select
                  className="bg-transparent border-none text-xs text-gray-500 font-bold outline-none cursor-pointer"
                  value={year}
                  onChange={(e) => setYear(parseInt(e.target.value))}
                >
                  <option value={currentYear}>{currentYear}</option>
                  <option value={currentYear - 1}>{currentYear - 1}</option>
                  <option value={currentYear - 2}>{currentYear - 2}</option>
                </select>
              </div>
            </div>
            <div className="h-80 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={revenue} barSize={28}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#F3F4F6" />
                  <XAxis
                    dataKey="name"
                    axisLine={false}
                    tickLine={false}
                    tick={{ fontSize: 12, fill: '#9CA3AF' }}
                    dy={10}
                  />
                  <YAxis
                    axisLine={false}
                    tickLine={false}
                    tick={{ fontSize: 12, fill: '#9CA3AF' }}
                  />
                  <Tooltip
                    cursor={{ fill: 'rgba(238, 80, 28, 0.05)' }}
                    contentStyle={{
                      borderRadius: '8px',
                      border: 'none',
                      boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                      backgroundColor: '#FFFFFF',
                      padding: '8px 12px',
                      fontSize: '12px',
                      fontWeight: 'bold'
                    }}
                    formatter={(value: number) => [`${formatCompactMoney(value)}`, 'Doanh thu']}
                  />
                  <Bar dataKey="value" fill="#EE501C" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Pie Chart */}
          <div className="bg-white p-6 rounded-2xl shadow-sm border border-gray-200 flex flex-col">
            <h3 className="text-lg font-bold text-gray-900 mb-6">Trạng thái đơn hàng</h3>
            <div className="h-64 w-full flex-1 relative">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={status}
                    cx="50%"
                    cy="50%"
                    innerRadius={70}
                    outerRadius={100}
                    paddingAngle={0}
                    dataKey="value"
                    startAngle={90}
                    endAngle={-270}
                  >
                    {status.map((entry: any, index: number) => (
                      <Cell key={`cell-${index}`} fill={entry.color} strokeWidth={2} stroke="#fff" />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 12px rgba(0,0,0,0.1)' }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-4 grid grid-cols-2 gap-y-3 gap-x-2">
              {status.map((item: any) => (
                <div key={item.name} className="flex items-center text-xs font-medium text-gray-500">
                  <span className="w-2.5 h-2.5 rounded-full mr-2 shrink-0" style={{ backgroundColor: item.color }}></span>
                  {item.name} ({item.value})
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Bottom Lists Row */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Activities */}
          <div className="bg-white p-6 rounded-2xl shadow-sm border border-gray-200">
            <h3 className="text-lg font-bold text-gray-900 mb-6">Hoạt động gần đây</h3>
            <div className="space-y-6">
              {activities.length === 0 ? (
                <p className="text-sm text-gray-400 text-center py-4">Chưa có hoạt động nào</p>
              ) : (
                activities.map((act: any) => (
                  <div key={act.id} className="flex items-start gap-4">
                    <div className={`w-8 h-8 rounded-full flex items-center justify-center shrink-0 
                      ${act.type === 'order' ? 'bg-blue-100 text-blue-600' :
                        act.type === 'delivery' ? 'bg-green-100 text-green-600' :
                          act.type === 'cancellation' ? 'bg-red-100 text-red-600' :
                            'bg-orange-100 text-orange-600'}`}>
                      <span className="text-xs font-bold">{getInitials(act.user || '?')}</span>
                    </div>
                    <div className="min-w-0 flex-1">
                      <p className="text-sm text-gray-800 leading-snug">
                        <span className="font-bold">{act.user || 'Unknown'}</span> {act.action || ''} <span className="text-[#EE501C] font-semibold">{act.target || ''}</span>
                      </p>
                      <p className="text-[10px] text-gray-400 mt-1">{act.time || ''}</p>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Top Items */}
          <div className="bg-white p-6 rounded-2xl shadow-sm border border-gray-200">
            <h3 className="text-lg font-bold text-gray-900 mb-6">Top món bán chạy</h3>
            <div className="space-y-5">
              {topItems.length === 0 ? (
                <p className="text-sm text-gray-400 text-center py-4">Chưa có dữ liệu</p>
              ) : (
                topItems.map((item: any) => {
                  // Tạo placeholder với chữ cái đầu của tên món
                  const initial = getInitials(item.name || '?');
                  const displayId = item.restaurantId || item.id || '';

                  return (
                    <div key={item.id || item.foodId} className="flex items-center gap-4">
                      <div className="w-12 h-12 rounded-lg flex items-center justify-center font-bold text-sm bg-gray-100 text-gray-600 shrink-0">
                        {initial}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-bold text-gray-800 truncate">{item.name}</p>
                        <p className="text-[10px] text-gray-400 mt-0.5">{displayId}</p>
                      </div>
                      <span className="text-sm font-bold text-[#EE501C] shrink-0">{item.sales}</span>
                    </div>
                  );
                })
              )}
            </div>
          </div>

          {/* Top Restaurants */}
          <div className="bg-white p-6 rounded-2xl shadow-sm border border-gray-200">
            <h3 className="text-lg font-bold text-gray-900 mb-6">Top doanh thu nhà hàng</h3>
            <div className="space-y-6">
              {topRestaurants.length === 0 ? (
                <p className="text-sm text-gray-400 text-center py-4">Chưa có dữ liệu</p>
              ) : (
                topRestaurants.map((res: any) => (
                  <div key={res.id} className="flex items-center gap-4">
                    <div className={`w-10 h-10 rounded-lg flex items-center justify-center font-bold text-sm shrink-0 ${res.color}`}>
                      {res.logoInitial}
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-bold text-gray-800 truncate">{res.name}</p>
                      <p className="text-[10px] text-gray-400 mt-0.5">{res.id}</p>
                    </div>
                    <div className="text-right shrink-0">
                      <p className="text-sm font-bold text-gray-900">
                        {formatCompactMoney(res.revenue)}
                      </p>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </AdminLayout>
  );
};
