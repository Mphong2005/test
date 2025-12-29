# middlewares/auth_middleware.py
from flask import request, jsonify
from functools import wraps
from core.security import security
from utils.roles import Role

def auth_required(f):
    """Decorator để bảo vệ routes cần authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # Lấy Authorization header
            auth_header = request.headers.get('Authorization')
            
            if not auth_header:
                return jsonify({'success': False,'message': 'Không tìm thấy token xác thực'}), 401
            
            # Extract token từ header
            token = security.extract_token_from_header(auth_header)
            
            if not token:
                return jsonify({'success': False,'message': 'Format token không hợp lệ. Sử dụng: Bearer <token>'}), 401
            
            # Verify token
            payload = security.verify_token(token)
            
            # Validate payload
            if not security.validate_token_payload(payload):
                return jsonify({'success': False,'message': 'Token không chứa đủ thông tin cần thiết'}), 401
            
            # Kiểm tra tài khoản có bị khóa không
            from services.user_service import user_service
            user = user_service.find_by_id(payload['user_id'])
            if user and not user.is_active:
                return jsonify({'success': False,'message': 'Tài khoản của bạn đã bị khóa'}), 403
            
            # Gắn user info vào request
            request.user_id = payload['user_id']
            request.user_email = payload['email']
            request.token_payload = payload  # Có thể cần thêm thông tin khác
            
            return f(*args, **kwargs)
            
        except ValueError as e:
            # Lỗi từ security.verify_token()
            return jsonify({'success': False,'message': str(e)}), 401  
        except Exception as e:
            # Lỗi không mong muốn
            return jsonify({'success': False,'message': 'Xác thực thất bại'}), 401
    return decorated_function


def optional_auth(f):
    """
    Decorator cho routes có thể có hoặc không có authentication
    Nếu có token hợp lệ → gắn user info vào request
    Nếu không có token hoặc token invalid → vẫn cho phép truy cập
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            auth_header = request.headers.get('Authorization')
            
            if auth_header:
                token = security.extract_token_from_header(auth_header)
                
                if token:
                    payload = security.verify_token(token)
                    
                    if security.validate_token_payload(payload):
                        request.user_id = payload['user_id']
                        request.user_email = payload['email']
                        request.token_payload = payload
            
        except Exception:
            # Nếu có lỗi, vẫn cho phép truy cập nhưng không có user info
            pass
        
        return f(*args, **kwargs)
    
    return decorated_function


def admin_required(f):
    """
    Decorator cho routes chỉ admin mới truy cập được
    Yêu cầu token phải có role='admin'
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            auth_header = request.headers.get('Authorization')
            
            if not auth_header:
                return jsonify({'success': False,'message': 'Không tìm thấy token xác thực'}), 401
            
            token = security.extract_token_from_header(auth_header)
            
            if not token:
                return jsonify({'success': False,'message': 'Format token không hợp lệ'}), 401
            
            payload = security.verify_token(token)
            
            # Kiểm tra tài khoản có bị khóa không
            from services.user_service import user_service
            user = user_service.find_by_id(payload['user_id'])
            if user and not user.is_active:
                return jsonify({'success': False,'message': 'Tài khoản của bạn đã bị khóa'}), 403
            
            # Check role
            token_role = payload.get('role')
            if token_role != Role.ADMIN.value:
                return jsonify({'success': False,'message': f'Bạn không có quyền truy cập. Role hiện tại: {token_role}'}), 403
            
            request.user_id = payload['user_id']
            request.user_email = payload['email']
            request.token_payload = payload
            
            return f(*args, **kwargs)
            
        except ValueError as e:
            return jsonify({'success': False,'message': str(e)}), 401
        except Exception as e:
            return jsonify({'success': False,'message': 'Xác thực thất bại'}), 401
    return decorated_function

def shipper_required(f):
    """
    Decorator cho routes chỉ shipper mới truy cập được
    Yêu cầu token phải có role='shipper'
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            auth_header = request.headers.get('Authorization')
            if not auth_header:
                return jsonify({'success': False,'message': 'Không tìm thấy token xác thực'}), 401

            token = security.extract_token_from_header(auth_header)
            if not token:
                return jsonify({'success': False,'message': 'Format token không hợp lệ'}), 401

            payload = security.verify_token(token)

            # Kiểm tra tài khoản có bị khóa không
            from services.user_service import user_service
            user = user_service.find_by_id(payload['user_id'])
            if user and not user.is_active:
                return jsonify({'success': False,'message': 'Tài khoản của bạn đã bị khóa'}), 403

            token_role = payload.get('role')
            if token_role != Role.SHIPPER.value:
                return jsonify({'success': False,'message': f'Bạn không có quyền truy cập. Role hiện tại: {token_role}'}), 403

            request.user_id = payload['user_id']
            request.user_email = payload['email']
            request.token_payload = payload

            return f(*args, **kwargs)
        except ValueError as e:
            return jsonify({'success': False,'message': str(e)}), 401
        except Exception:
            return jsonify({'success': False,'message': 'Xác thực thất bại'}), 401
    return decorated_function


def user_required(f):
    """
    Decorator cho routes chỉ USER mới truy cập được
    - Yêu cầu token hợp lệ
    - role phải là 'user'
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            auth_header = request.headers.get('Authorization')
            if not auth_header:
                return jsonify({'success': False, 'message': 'Không tìm thấy token xác thực'}), 401

            token = security.extract_token_from_header(auth_header)
            if not token:
                return jsonify({'success': False, 'message': 'Format token không hợp lệ'}), 401

            payload = security.verify_token(token)

            # Kiểm tra tài khoản có bị khóa không
            from services.user_service import user_service
            user = user_service.find_by_id(payload['user_id'])
            if user and not user.is_active:
                return jsonify({'success': False, 'message': 'Tài khoản của bạn đã bị khóa'}), 403

            token_role = payload.get('role')
            if token_role != Role.USER.value:
                return jsonify({'success': False, 'message': f'Bạn không có quyền truy cập. Role hiện tại: {token_role}'}), 403

            request.user_id = payload['user_id']
            request.user_email = payload['email']
            request.token_payload = payload

            return f(*args, **kwargs)
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 401
        except Exception:
            return jsonify({'success': False, 'message': 'Xác thực thất bại'}), 401

    return decorated_function


def user_or_admin_required(f):
    """
    Decorator cho routes mà cả USER và ADMIN đều truy cập được
    - Yêu cầu token hợp lệ
    - role phải là 'user' hoặc 'admin'
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            auth_header = request.headers.get('Authorization')
            if not auth_header:
                return jsonify({'success': False, 'message': 'Không tìm thấy token xác thực'}), 401

            token = security.extract_token_from_header(auth_header)
            if not token:
                return jsonify({'success': False, 'message': 'Format token không hợp lệ'}), 401

            payload = security.verify_token(token)

            # Kiểm tra tài khoản có bị khóa không
            from services.user_service import user_service
            user = user_service.find_by_id(payload['user_id'])
            if user and not user.is_active:
                return jsonify({'success': False, 'message': 'Tài khoản của bạn đã bị khóa'}), 403

            token_role = payload.get('role')
            if token_role not in [Role.USER.value, Role.ADMIN.value]:
                return jsonify({'success': False, 'message': f'Bạn không có quyền truy cập. Role hiện tại: {token_role}'}), 403

            request.user_id = payload['user_id']
            request.user_email = payload['email']
            request.token_payload = payload

            return f(*args, **kwargs)
        except ValueError as e:
            return jsonify({'success': False, 'message': str(e)}), 401
        except Exception:
            return jsonify({'success': False, 'message': 'Xác thực thất bại'}), 401

    return decorated_function

    