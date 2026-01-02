# Food Delivery Frontend - React Application

## 📋 Giới thiệu

Frontend của FoodDelivery sử dụng **React 19** + **TypeScript** + **Vite** framework. Hỗ trợ 3 roles: Admin, User, Shipper.

## 🏗️ Kiến trúc Project

```
frontend/src/
├── App.tsx                       # 🚦 Router configuration & route definitions
├── index.tsx                     # 🚀 Entry point
├── index.html                    # HTML template
├── vite.config.ts                # Vite configuration
├── tsconfig.json                 # TypeScript configuration
├── package.json                  # Dependencies
│
├── api/                          # 📡 API endpoints & HTTP client
│   ├── axiosClient.ts            # Axios instance with JWT interceptor
│   ├── authApi.ts                # POST /auth/login, /auth/register, /auth/refresh
│   ├── userApi.ts                # GET/PUT user profile, balance, address
│   ├── restaurantApi.ts          # GET restaurants, menus, details
│   ├── orderApi.ts               # POST/GET/DELETE orders
│   ├── cartApi.ts                # GET/POST/DELETE cart items
│   ├── paymentApi.ts             # POST create payment, confirm, refund
│   ├── reviewApi.ts              # POST/GET reviews
│   ├── voucherApi.ts             # GET vouchers, validate code
│   ├── dashboardApi.ts           # GET admin statistics
│   └── shipperApi.ts             # Shipper order management
│
├── services/                     # 🔧 Business logic layer
│   ├── authService.ts            # Login, logout, token management, refresh
│   ├── userService.ts            # User profile, balance, address management
│   └── orderService.ts           # Order transform & helper functions
│
├── hooks/                        # 🪝 Custom React hooks
│   ├── useAuth.ts                # Auth state management
│   ├── useUser.ts                # User data management
│   ├── useOrder.ts               # User order list management
│   ├── useRestaurant.ts          # Restaurant data fetching
│   ├── useAdminOrder.ts          # Admin order list management
│   ├── useShipperOrder.ts        # Shipper order tracking
│   └── useVoucher.ts             # Voucher management
│
├── contexts/                     # 🌐 Global state management
│   ├── AuthContext.tsx           # Auth provider, useAuth hook
│   └── CartContext.tsx           # Cart provider, useCart hook
│
├── utils/                        # 🛠️ Utility functions
│   ├── formatters.ts             # Date, currency, phone formatters
│   ├── storage.ts                # localStorage token management
│   ├── validation.ts             # Email, phone validation
│   ├── roles.ts                  # Role enum constants
│   └── index.ts                  # Export all utilities
│
├── layouts/                      # 📐 Layout wrappers
│   ├── MainLayout.tsx            # User pages (Header + Footer)
│   ├── AdminLayout.tsx           # Admin pages (Sidebar + Header)
│   ├── ShipperLayout.tsx         # Shipper pages (Sidebar + Header)
│   └── AuthLayout.tsx            # Auth pages (Login, Register)
│
├── components/                   # 🎨 Reusable UI components
│   ├── common/
│   │   ├── Header.tsx            # Navigation header
│   │   ├── Footer.tsx            # Footer
│   │   ├── Sidebar.tsx           # Admin/Shipper sidebar navigation
│   │   ├── Modal.tsx             # Generic modal wrapper
│   │   ├── Loading.tsx           # Loading spinner
│   │   └── ErrorMessage.tsx      # Error display
│   ├── admin/
│   │   ├── UserManagement.tsx    # Users table & actions
│   │   ├── RestaurantManagement.tsx
│   │   ├── OrderManagement.tsx
│   │   ├── StatisticsCard.tsx
│   │   └── Charts.tsx
│   ├── user/
│   │   ├── RestaurantCard.tsx    # Restaurant display card
│   │   ├── FoodCard.tsx          # Food item card
│   │   ├── CartItem.tsx          # Cart item display
│   │   └── ReviewModal.tsx       # Review submission modal
│   ├── shipper/
│   │   ├── OrderTracker.tsx      # Order tracking map
│   │   ├── DeliveryList.tsx      # Shipper order list
│   │   └── StatusBadge.tsx       # Order status badge
│   └── routes/
│       ├── AdminRoute.tsx        # Protected route for admin
│       ├── ShipperRoute.tsx      # Protected route for shipper
│       └── UserRoute.tsx         # Protected route for user
│
├── page/                         # 📄 Route page components
│   ├── auth/
│   │   ├── LoginPage.tsx         # User login
│   │   ├── RegisterPage.tsx      # User registration
│   │   └── ForgotPasswordPage.tsx
│   ├── user/
│   │   ├── HomePage.tsx          # Restaurant list & browse
│   │   ├── RestaurantPage.tsx    # Restaurant menu & details
│   │   ├── ProductDetailPage.tsx # Food item detail & add to cart
│   │   ├── CheckoutPage.tsx      # Order confirmation
│   │   ├── OrdersPage.tsx        # User order history & tracking
│   │   ├── ProfilePage.tsx       # User profile settings
│   │   └── ReviewPage.tsx        # Review list & management
│   ├── admin/
│   │   ├── AdminDashboard.tsx    # Admin statistics & overview
│   │   ├── AdminUsers.tsx        # User management
│   │   ├── AdminRestaurants.tsx  # Restaurant management
│   │   └── AdminOrders.tsx       # Order management
│   └── shipper/
│       ├── ShipperDashboard.tsx  # Delivery list
│       └── DeliveryDetail.tsx    # Order detail & tracking
│
├── types/                        # 📝 TypeScript interfaces
│   └── common.ts                 # All type definitions
│       ├── User, Restaurant, Order, Payment, Review interfaces
│       └── API request/response types
│
├── assets/                       # 🖼️ Static files
│   └── images/
│
└── vite-env.d.ts                 # Vite type definitions
```

## 🔐 Authentication

### Login Flow

1. User enters email + password on LoginPage
2. Frontend calls `POST /api/auth/login`
3. Backend returns `access_token` + `refresh_token`
4. Frontend stores tokens in localStorage via `storage.ts`
5. `AuthContext` provides `useAuth()` hook for global auth state

### Protected Routes

- Admin pages: `<AdminRoute />` component wraps admin pages
- Shipper pages: `<ShipperRoute />` component wraps shipper pages
- User pages: `<UserRoute />` component wraps user pages
- Each route checks JWT token & user role before rendering

### Token Management

```typescript
// storage.ts - localStorage operations
saveToken(accessToken)       // Save access token
saveRefreshToken(refreshToken)
getToken()                   // Retrieve access token
removeItem()                 // Clear tokens on logout

// axiosClient.ts - Auto JWT header
// All requests automatically include: Authorization: Bearer {token}

// authService.ts - Token refresh
refreshAccessToken()         // Request new token using refresh token
```

## 📡 API Communication

### Axios Client Setup

All API requests go through [api/axiosClient.ts](src/api/axiosClient.ts):
- Base URL: `VITE_BACKEND_URL` or default `http://127.0.0.1:5000`
- Auto JWT header injection
- Request/response interceptors
- Error handling

### API Layers

```
Page/Component
    ↓
Hooks (useAuth, useOrder, etc)
    ↓
Services (orderService, userService)
    ↓
API functions (orderApi, userApi)
    ↓
Axios Client
    ↓
Backend API
```

### Example API Call

```typescript
// ProductDetailPage.tsx
import { addItemToCartApi } from '@/api/cartApi';
import { useAuth } from '@/hooks/useAuth';

export function ProductDetailPage() {
  const { user } = useAuth();
  
  const handleAddToCart = async (foodName, quantity) => {
    try {
      const response = await addItemToCartApi({
        restaurantId: restaurantId,
        foodName,
        quantity
      });
      // Success - item added to cart
    } catch (error) {
      // Error handling
    }
  };
}
```

## 🎯 Core Features

### User Features
- 🔐 Register/Login with JWT
- 🍜 Browse restaurants & menus
- 🛒 Add/remove items from cart
- 💰 View balance, top-up balance
- 📦 Create orders (COD or Balance payment)
- 📍 Track order status in real-time
- ⭐ Review restaurants & food items
- 🎟️ Apply voucher codes
- 👤 Manage profile & address

### Admin Features
- 📊 Dashboard with statistics
- 👥 User management
- 🏪 Restaurant management
- 📋 Order monitoring
- 💳 Payment tracking
- 📈 Revenue reports

### Shipper Features
- 📋 Delivery list
- 🗺️ Route optimization
- 📍 Real-time tracking
- ✅ Delivery confirmation

## 🌐 State Management

### AuthContext
```typescript
// Global authentication state
<AuthProvider>
  <App />
</AuthProvider>

// Usage anywhere
const { user, token, login, logout, isAuthenticated } = useAuth();
```

### CartContext
```typescript
// Global cart state
<CartProvider>
  <App />
</CartProvider>

// Usage in pages
const { cartItems, addItem, removeItem, totalPrice } = useCart();
```

### Component State
- useState for component-level state
- useEffect for side effects (data fetching)

## 🎨 Styling

- Tailwind CSS for utility classes
- Custom colors in tailwind.config.js
- Component-scoped styles via className

## 🧪 Testing

### Mock Login Credentials

```
Admin:
  Email: admin@gmail.com
  Password: 123456

User:
  Email: user@gmail.com
  Password: 123456

Shipper:
  Email: shipper@food.com
  Password: 123456
```

### Test Flows

1. **User Order Flow**
   - Login → Browse restaurants → Add to cart → Checkout → Order → Track → Review

2. **Admin Dashboard**
   - Login as admin → View statistics → Manage users → Manage restaurants

3. **Shipper Delivery**
   - Login as shipper → View deliveries → Update status → Confirm delivery

## 🐛 Common Issues

### Backend connection failed
- Check VITE_BACKEND_URL matches backend server
- Verify backend is running (`python app/main.py`)
- Check browser console for CORS errors

### JWT Token expired
- Frontend auto-refreshes using refresh_token
- If refresh fails → user logged out
- Check JWT_SECRET in backend .env

### Cart items not persisting
- CartContext state is in-memory (not localStorage)
- Cart persists while session is active
- Clear on logout (by design)

## 📚 Technologies

- **Framework**: React 19.2.3
- **Language**: TypeScript 5.8.2
- **Build Tool**: Vite 7.3.0
- **Router**: React Router DOM 7.11.0
- **HTTP**: Axios 1.13.2
- **UI**: Tailwind CSS
- **Icons**: Lucide React
- **Token Decode**: jwt-decode 4.0.0
- **Charts**: Recharts 3.6.0

## 🚀 Docker Deployment

```bash
# Build frontend image
docker build -f frontend/Dockerfile -t fooddelivery-frontend:latest .

# Run with docker-compose
docker-compose up --build

# Frontend accessible at http://localhost
```

## 📝 Environment Variables

```
VITE_BACKEND_URL               # Backend API URL (default: http://127.0.0.1:5000)
```

## 📞 File Structure Tips

- **Page files**: Main route components (HomePage, LoginPage, etc)
- **Components**: Reusable UI components
- **Hooks**: Custom logic hooks (always return something)
- **Services**: API call wrappers + business logic
- **API**: Raw HTTP calls to backend
- **Utils**: Pure utility functions

## 📖 Resources

- React Docs: https://react.dev
- Vite Docs: https://vitejs.dev
- TypeScript: https://typescriptlang.org
- Tailwind CSS: https://tailwindcss.com