
export enum OrderStatus {
  Pending = 'PENDING',
  Delivering = 'DELIVERING',
  Completed = 'COMPLETED',
  Cancelled = 'CANCELLED'
}

export enum Page {
  Home = 'home',
  History = 'history',
  Pending = 'pending',
  Profile = 'profile'
}

export interface OrderItem {
  name: string;
  quantity: number;
  price: number;
}

export interface ShipperCustomer {
  name: string;
  phone: string;
  email?: string;
  avatar?: string;
  rank?: string;
}

export interface ShipperOrder {
  id: string;
  storeName: string;
  foodName?: string; // Tên món đầu tiên để hiển thị
  storeImage: string;
  storeAddress: string;
  deliveryAddress: string;
  status: OrderStatus;
  paymentMethod: 'Cash' | 'Wallet';
  time: string; // Thời gian giao (completed) hoặc thời gian đặt (pending/shipping)
  orderTime?: string; // Thời gian đặt đơn (luôn là createdAt)
  totalAmount: number;
  items: OrderItem[];
  customer?: ShipperCustomer;
}

export interface ShipperProfile {
  name: string;
  email: string;
  avatar: string;
  rank: string;
  joinDate: string;
  phone: string;
  address: string;
  dob: string;
}

// ShipperInfo - dùng cho ContactShipperModal
export interface ShipperInfo {
  name: string;
  id: string;
  phone: string;
  email: string;
  avatar?: string;
}
