# 🍔 Food Delivery System

Hệ thống đặt đồ ăn trực tuyến đầy đủ tính năng, hỗ trợ 3 vai trò người dùng (User, Shipper, Admin) với quản lý đơn hàng real-time, thanh toán và đánh giá.

---

## 🛠️ Tech Stack

### **Frontend**
- React 19 + TypeScript
- Vite (Build tool & Dev server)
- React Router v7 (Routing)
- Axios (HTTP client)
- Recharts (Data visualization)

### **Backend**
- Flask (Python web framework)
- PyMongo (MongoDB ODM)
- JWT Authentication
- Pydantic (Data validation)
- Flask-CORS

### **Database & DevOps**
- MongoDB Atlas
- Docker & Docker Compose

---

## ✨ Key Features

### 👤 **User (Customer)**
- Đăng ký/Đăng nhập với JWT
- Browse nhà hàng & menu món ăn
- Quản lý giỏ hàng
- Áp dụng voucher giảm giá
- Đặt hàng và thanh toán (Online/COD)
- Tracking đơn hàng real-time
- Đánh giá & review nhà hàng
- Nạp tiền vào tài khoản

### 🚚 **Shipper**
- Xem danh sách đơn hàng chờ giao
- Nhận đơn hàng
- Cập nhật trạng thái giao hàng
- Lịch sử giao hàng
- Xem thu nhập

### 👨‍💼 **Admin**
- Dashboard tổng quan hệ thống
- Quản lý users (customer & shipper)
- Tạo và quản lý nhà hàng
- Quản lý menu món ăn
- Quản lý vouchers/promotions
- Xem tất cả đơn hàng
- Báo cáo doanh thu & thống kê
- Kích hoạt/vô hiệu hóa nhà hàng và users

---

## 📁 Project Structure


**📊 [View Database ERD](ERD.pdf)**

```
Project_WebCK/
├── app/                          # Backend (Flask)
│   ├── controllers/              # Business logic
│   ├── routes/                   # API endpoints
│   ├── services/                 # Service layer
│   ├── db/models/                # MongoDB models
│   ├── schemas/                  # Pydantic schemas
│   ├── middlewares/              # Auth middleware
│   ├── core/                     # Config & security
│   └── main.py                   # Entry point
├── frontend/src/                 # Frontend (React + TypeScript)
│   ├── api/                      # API clients
│   ├── components/               # UI components
│   ├── contexts/                 # Auth & Cart contexts
│   ├── hooks/                    # Custom hooks
│   ├── pages/                    # Page components
│   └── routes/                   # Route config
├── docker-compose.yml            # Container orchestration
├── requirements.txt              # Python dependencies
├── ERD.pdf                       # Entity Relationship Diagram
└── .env.example                  # Environment template
```

---

## 🚀 Getting Started

### **Prerequisites**
- Docker & Docker Compose (Recommended)
- Hoặc: Python 3.12+ và Node.js 20+
- MongoDB Atlas account (hoặc local MongoDB)

### **1. Clone Repository**
```bash
git clone <repository-url>
cd Project_WebCK
```

### **2. Environment Configuration**
Tạo file `.env` từ template và điền thông tin:
```bash
cp .env.example .env
```

Cập nhật các giá trị trong `.env`:
```env
MONGO_URI=mongodb+srv://username:password@cluster.mongodb.net/
MONGO_DB_NAME=fooddelivery
JWT_SECRET=your-secret-key-here
SECRET_KEY=your-flask-secret-key
```

### **3. Run with Docker (Recommended)**

```bash
# Build và start tất cả services
docker-compose up --build

# Chạy ở chế độ background
docker-compose up -d

# Xem logs
docker-compose logs -f

# Stop services
docker-compose down
```

**Services chạy tại:**
- 🌐 Frontend: **http://localhost:3000**
- 🔧 Backend API: **http://localhost:5000**

### **4. Run Locally (Alternative)**

#### **Backend**
```bash
cd app
pip install -r ../requirements.txt
python main.py
```

#### **Frontend**
```bash
cd frontend/src
npm install
npm run dev
```

---

## 📡 API Documentation

### **Authentication**
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/register` | Đăng ký user mới |
| POST | `/api/auth/login` | Đăng nhập (trả về JWT token) |
| POST | `/api/auth/refresh` | Refresh access token |

### **Main Resources**
| Resource | Endpoint | Description |
|----------|----------|-------------|
| Users | `/api/users` | Profile, balance, quản lý users |
| Restaurants | `/api/restaurants` | CRUD nhà hàng & menu |
| Orders | `/api/orders` | Tạo, tracking, cập nhật đơn hàng |
| Cart | `/api/cart` | Quản lý giỏ hàng |
| Payments | `/api/payments` | Xử lý thanh toán |
| Vouchers | `/api/vouchers` | Mã giảm giá |
| Reviews | `/api/reviews` | Đánh giá nhà hàng |
| Dashboard | `/api/dashboard` | Thống kê (Admin only) |

**Authentication:** API sử dụng JWT Bearer token
```
Authorization: Bearer <your_access_token>
```

**Health Check:**
```bash
curl http://localhost:5000/health
```

---

## 🔐 User Roles

Hệ thống có **3 roles** với quyền hạn khác nhau:

| Role | Value | Quyền hạn |
|------|-------|-----------|
| **Admin** | `admin` | Toàn quyền quản lý hệ thống, nhà hàng, users, vouchers, thống kê |
| **User** | `user` | Đặt món, thanh toán, đánh giá (customer) |
| **Shipper** | `shipper` | Nhận và giao đơn hàng |

**Lưu ý:** Role `admin` không thể đăng ký qua API, chỉ được set trực tiếp trong database.

---

## 🧪 Development

### **Xem Logs**
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend
docker-compose logs -f frontend
```

### **Restart Services**
```bash
docker-compose restart backend
docker-compose restart frontend
```

### **Rebuild After Code Changes**
```bash
docker-compose up --build
```

---

## 📊 Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGO_URI` | MongoDB connection string | **Required** |
| `MONGO_DB_NAME` | Database name | `fooddelivery` |
| `JWT_SECRET` | Secret key for JWT signing | **Required** |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | Token expiry time | `60` |
| `SECRET_KEY` | Flask secret key | **Required** |
| `DEBUG` | Enable debug mode | `false` |
| `VITE_BACKEND_URL` | Backend URL for frontend | `http://localhost:5000` |

---

## 🤝 Contributors

**Nhóm CK** - Web Development Course

---

## 📝 License

Educational project - Đồ án cuối kỳ môn Phát triển Ứng dụng Web

---

## 🐛 Troubleshooting

### **Frontend không kết nối được Backend**
- Kiểm tra backend có đang chạy: `docker-compose ps`
- Test health endpoint: `curl http://localhost:5000/health`
- Kiểm tra biến `VITE_BACKEND_URL` trong `.env`

### **Backend lỗi MongoDB connection**
- Kiểm tra `MONGO_URI` trong file `.env`
- Whitelist IP trong MongoDB Atlas Network Access
- Test connection string trên MongoDB Compass

### **Port đã được sử dụng**
- Đổi port trong `docker-compose.yml`:
  ```yaml
  ports:
    - "3001:3000"  # Frontend
    - "5001:5000"  # Backend
  ```

---