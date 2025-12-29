# Frontend Structure

Thư mục `frontend/` chứa toàn bộ phần giao diện của dự án FoodDelivery.

## Cấu trúc thư mục

```
frontend/src/
├── api/                      # 📡 API endpoints & HTTP client
│   ├── index.ts              # Base URL, requestJson, getAuthHeaders
│   ├── authApi.ts            # login, register APIs
│   └── userApi.ts            # User CRUD APIs
│
├── services/                 # 🔧 Business logic layer
│   ├── authService.ts        # Auth logic (login, logout, token utils)
│   └── userService.ts        # User management logic
│
├── hooks/                    # 🪝 Custom React hooks
│   └── useUserManagement.ts  # Hook quản lý users cho Admin
│
├── contexts/                 # 🌐 Global state management
│   └── AuthContext.tsx       # AuthProvider, useAuth hook
│
├── utils/                    # 🛠️ Helper functions
│   ├── validation.ts         # Email, phone, password validation
│   └── formatters.ts         # Date, currency formatters
│
├── layouts/                  # 📐 Layout wrappers
│   ├── MainLayout.tsx        # User pages (Header + Footer)
│   └── AdminLayout.tsx       # Admin pages (Sidebar)
│
├── components/               # 🎨 UI Components
│   ├── Header.tsx
│   ├── Footer.tsx
│   ├── Sidebar.tsx
│   ├── Button.tsx
│   ├── Modal.tsx
│   ├── AuthLayout.tsx
│   ├── AdminRoute.tsx
│   └── admin/
│       ├── UserTable.tsx
│       └── AdminModals.tsx
│
├── page/                     # 📄 Route pages
│   ├── HomePage.tsx
│   ├── LoginPage.tsx
│   ├── RegisterPage.tsx
│   ├── ForgotPasswordPage.tsx
│   └── admin/
│       └── AdminDashboard.tsx
│
├── types/                    # 📝 TypeScript interfaces
│   └── admin.ts
│
├── App.tsx                   # 🚦 Router configuration
└── index.tsx                 # 🚀 Entry point
```

## Chạy Frontend

```bash
cd frontend/src
npm install
npm run dev
```

## Build Production

```bash
npm run build
```

## Cấu hình API endpoint

Tạo file `.env.local` trong `frontend/src/` nếu muốn chỉ định backend khác:

```
VITE_BACKEND_URL=http://localhost:5000
```

Nếu không cấu hình, frontend sẽ gọi `http://127.0.0.1:5000`.

## Luồng dữ liệu

```
Pages/Components → Hooks → Services → API → Backend
```

