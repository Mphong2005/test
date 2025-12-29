from flask import Blueprint
from controllers.user_controller import user_controller

auth_router = Blueprint("auth_router", __name__)

@auth_router.route("/register", methods=["POST"])
def register():
    """POST /api/auth/register - Đăng ký tài khoản"""
    return user_controller.register()

@auth_router.route("/login", methods=["POST"])
def login():
    """POST /api/auth/login - Đăng nhập"""
    return user_controller.login()

@auth_router.route("/refresh", methods=["POST"])
def refresh():
    """POST /api/auth/refresh - Làm mới access token"""
    return user_controller.refresh_token()