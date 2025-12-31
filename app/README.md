# Food Delivery Backend - API Documentation

## 📋 Giới thiệu

Backend của FoodDelivery sử dụng **Flask** framework với **MongoDB** database. Hỗ trợ authentication JWT, role-based access control (Admin, User, Shipper).

## 🏗️ Kiến trúc Project

```
app/
├── main.py                        # 🚀 Entry point - Flask app configuration
├── requirements.txt               # 📦 Python dependencies
├── Dockerfile                     # 🐳 Docker configuration
├── .dockerignore                  # 🚫 Files to exclude from Docker build
│
├── core/                          # ⚙️ Core configuration & security
│   ├── __init__.py
│   ├── config.py                  # Environment variables configuration
│   └── security.py                # JWT token creation/verification
│
├── db/                            # 🗄️ Database connection & models
│   ├── connection.py              # MongoDB connection & indexes
│   └── models/
│       ├── user.py                # User model (id, email, password, role, etc)
│       ├── restaurants.py         # Restaurant model (menu, address, etc)
│       ├── order.py               # Order model (items, status, payment)
│       ├── payment.py             # Payment model (method, amount, status)
│       ├── cart.py                # Cart model (user items)
│       ├── review.py              # Review model (rating, comment, order)
│       ├── voucher.py             # Voucher model (discount, conditions)
│       ├── common.py              # Base classes (MongoDBModel, etc)
│       └── __init__.py
│
├── routes/                        # 🛣️ API endpoints
│   ├── auth_route.py              # POST /auth/login, /auth/register, /auth/refresh
│   ├── user_route.py              # GET/PUT /users (profile, address, balance)
│   ├── restaurant_route.py        # GET /restaurants (list, filter, menu)
│   ├── order_route.py             # POST/GET /orders (create, list, cancel)
│   ├── payment_route.py           # POST /payments (create, confirm, refund)
│   ├── cart_route.py              # POST/GET /cart (add, remove, list items)
│   ├── review_route.py            # POST/GET /reviews (create, list reviews)
│   ├── voucher_route.py           # GET /vouchers (list, validate)
│   ├── dashboard_route.py         # GET /dashboard (admin stats)
│   └── __init__.py
│
├── controllers/                   # 🎮 Request handlers
│   ├── user_controller.py
│   ├── auth_controller.py
│   ├── restaurant_controller.py
│   ├── order_controller.py
│   ├── payment_controller.py
│   ├── cart_controller.py
│   ├── review_controller.py
│   ├── voucher_controller.py
│   ├── dashboard_controller.py
│   └── __init__.py
│
├── services/                      # 📌 Business logic layer
│   ├── user_service.py            # User CRUD, authentication logic
│   ├── restaurant_service.py      # Restaurant data management
│   ├── order_service.py           # Order creation, status management
│   ├── payment_service.py         # Payment processing logic
│   ├── cart_service.py            # Cart item management
│   ├── review_service.py          # Review creation, retrieval
│   ├── voucher_service.py         # Voucher validation, discount calc
│   ├── dashboard_service.py       # Admin statistics
│   ├── __init__.py
│   └── base_service.py            # Base service class
│
├── schemas/                       # 📊 Request/Response validation
│   ├── user_schema.py             # User DTO (login, register, profile)
│   ├── restaurant_schema.py
│   ├── order_schema.py
│   ├── payment_schema.py
│   ├── cart_schema.py
│   ├── review_schema.py
│   ├── voucher_schema.py
│   ├── dashboard_schema.py
│   └── __init__.py
│
├── middlewares/                   # 🔐 Middleware functions
│   ├── auth_middleware.py         # JWT token verification
│   └── __init__.py
│
└── utils/                         # 🛠️ Utility functions
    ├── roles.py                   # Role enums (Admin, User, Shipper)
    ├── mongo_parser.py            # ObjectId parsing
    └── __init__.py
```

## 🔧 Setup & Installation

### Prerequisites
- Python 3.12+
- MongoDB Atlas account
- Git

### Local Development

```bash
# 1. Clone repository
cd Project_WebCK

# 2. Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (macOS/Linux)
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Setup environment variables
cp .env.example .env
# Edit .env with your MongoDB URI and JWT secret

# 5. Run backend
python app/main.py

# Backend will run at http://127.0.0.1:5000
```

### Docker Setup

```bash
# 1. Setup environment
cp .env.example .env
# Edit .env with MongoDB credentials

# 2. Build and run with docker-compose
docker-compose up --build

# Access at http://127.0.0.1:5000
```

## 🔐 Authentication

### JWT Token Flow

1. **Login/Register** → Backend tạo `access_token` (1 day) + `refresh_token` (7 days)
2. **Frontend** lưu tokens vào localStorage
3. **API Requests** gửi kèm: `Authorization: Bearer {access_token}`
4. **Token expired** → Frontend dùng `refresh_token` để lấy `access_token` mới

### Roles & Permissions

```python
# roles.py
- Admin      # Quản lý restaurants, users, orders, dashboard
- User       # Order, review, cart management
- Shipper    # Order tracking, delivery management
```

### Protected Routes (require JWT)

```
GET  /api/users/{id}         # Get user profile (self or admin)
PUT  /api/users/{id}         # Update profile
GET  /api/orders             # List user orders
POST /api/orders             # Create order
GET  /api/reviews            # List reviews
POST /api/reviews            # Create review
...
```

## 📡 API Endpoints Overview

### Auth
- `POST /api/auth/login` - User login
- `POST /api/auth/register` - New user registration
- `POST /api/auth/refresh` - Refresh access token

### Users
- `GET /api/users/{id}` - Get user profile
- `PUT /api/users/{id}` - Update profile
- `GET /api/users/{id}/balance` - Check balance
- `POST /api/users/{id}/top-up` - Top up balance

### Restaurants
- `GET /api/restaurants` - List restaurants (with pagination, filter)
- `GET /api/restaurants/{id}` - Get restaurant details
- `GET /api/restaurants/{id}/menu` - Get restaurant menu

### Orders
- `POST /api/orders` - Create order
- `GET /api/orders` - List user orders
- `GET /api/orders/{id}` - Get order details
- `PUT /api/orders/{id}/status` - Update order status
- `DELETE /api/orders/{id}` - Cancel order

### Payments
- `POST /api/payments` - Create payment
- `GET /api/payments/{id}` - Get payment details
- `POST /api/payments/{id}/confirm` - Confirm payment

### Cart
- `GET /api/cart` - Get cart items
- `POST /api/cart` - Add to cart
- `DELETE /api/cart/{itemId}` - Remove from cart
- `PUT /api/cart/{itemId}` - Update item quantity

### Reviews
- `POST /api/reviews` - Create review
- `GET /api/reviews?restaurantId=xxx` - List reviews

### Vouchers
- `GET /api/vouchers` - List available vouchers
- `POST /api/vouchers/validate` - Validate voucher code

### Dashboard (Admin only)
- `GET /api/dashboard/stats` - Get statistics
- `GET /api/dashboard/orders` - List all orders
- `GET /api/dashboard/users` - List all users

## 🗄️ Database Schema

### Collections

**users**
```javascript
{
  _id: ObjectId,
  email: string,
  password: string (hashed),
  fullname: string,
  phone_number: string,
  address: string,
  avatar_url: string,
  balance: number,
  role: enum ['Admin', 'User', 'Shipper'],
  is_active: boolean,
  created_at: date,
  updated_at: date
}
```

**restaurants**
```javascript
{
  _id: ObjectId,
  restaurant_name: string,
  address: string,
  hotline: string,
  average_rating: number,
  menu: [
    {
      category_name: string,
      items: [
        {
          name: string,
          price: number,
          image: string,
          description: string
        }
      ]
    }
  ],
  created_at: date
}
```

**orders**
```javascript
{
  _id: ObjectId,
  user_id: ObjectId,
  restaurant_id: ObjectId,
  items: [
    {
      food_name: string,
      quantity: number,
      unit_price: number,
      subtotal: number
    }
  ],
  status: enum ['Pending', 'Confirmed', 'Preparing', 'Delivering', 'Completed', 'Cancelled'],
  total_amount: number,
  payment_method: enum ['COD', 'Balance'],
  payment_status: enum ['Pending', 'Completed', 'Failed'],
  shipping_address: string,
  shipper_id: ObjectId,
  notes: string,
  is_reviewed: boolean,
  created_at: date,
  updated_at: date
}
```

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

### Test Endpoints

```bash
# Login
curl -X POST http://127.0.0.1:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@gmail.com",
    "password": "123456"
  }'

# Get user profile
curl -X GET http://127.0.0.1:5000/api/users/userId \
  -H "Authorization: Bearer {access_token}"

# Health check
curl http://127.0.0.1:5000/health
```

## 🐛 Common Issues

### MongoDB connection failed
- Check MONGO_URI format
- Verify MongoDB Atlas IP whitelist (allow 0.0.0.0/0)
- Check network connectivity

### JWT secret not set
- Ensure JWT_SECRET in .env
- Restart backend after changing

### Port already in use
```bash
# Find process using port 5000
netstat -ano | findstr :5000

# Kill process
taskkill /PID <PID> /F
```

## 📚 Technologies

- **Framework**: Flask 2.x
- **Database**: MongoDB with PyMongo
- **Authentication**: JWT (PyJWT)
- **Validation**: Pydantic
- **Password Hashing**: Flask-Bcrypt
- **CORS**: Flask-CORS

## 🚀 Deployment

### Using Docker

```bash
# Build image
docker build -f app/Dockerfile -t fooddelivery-backend:latest .

# Run container
docker run -p 5000:5000 \
  -e MONGO_URI=your_uri \
  -e JWT_SECRET=your_secret \
  fooddelivery-backend:latest
```

### Using docker-compose

```bash
docker-compose up --build
```

## 📝 Environment Variables

```
MONGO_URI                      # MongoDB connection string
MONGO_DB_NAME                  # Database name (default: fooddelivery)
JWT_SECRET                     # Secret key for JWT signing
JWT_ALGORITHM                  # JWT algorithm (default: HS256)
ACCESS_TOKEN_EXPIRE_MINUTES    # Access token TTL (default: 1440 = 1 day)
REFRESH_TOKEN_EXPIRE_DAYS      # Refresh token TTL (default: 7)
SECRET_KEY                     # Flask secret key
DEBUG                          # Debug mode (true/false)
HOST                           # Server host (default: 0.0.0.0)
PORT                           # Server port (default: 5000)
```