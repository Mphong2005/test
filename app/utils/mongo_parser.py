"""
Utility functions to parse MongoDB Extended JSON format
Handles: {"$date": "..."}, {"$oid": "..."}, etc.
"""
from datetime import datetime
from typing import Any, Dict
from bson import ObjectId


def parse_mongo_date(value: Any) -> Any:
    """
    Parse MongoDB Extended JSON date format: {"$date": "2025-12-01T00:00:00.000Z"}
    Returns datetime object if valid, otherwise returns original value
    """
    if isinstance(value, dict):
        if '$date' in value:
            date_str = value['$date']
            try:
                # Parse ISO format date string
                if isinstance(date_str, str):
                    # Handle both "2025-12-01T00:00:00.000Z" and "2025-12-01T00:00:00.000+00:00"
                    if date_str.endswith('Z'):
                        date_str = date_str[:-1] + '+00:00'
                    return datetime.fromisoformat(date_str)
                elif isinstance(date_str, datetime):
                    return date_str
            except Exception as e:
                print(f"Error parsing date {value}: {e}")
                return value
    elif isinstance(value, str):
        # Try to parse ISO string directly
        try:
            if value.endswith('Z'):
                value = value[:-1] + '+00:00'
            return datetime.fromisoformat(value)
        except:
            pass
    
    return value


def parse_mongo_oid(value: Any) -> Any:
    """
    Parse MongoDB Extended JSON ObjectId format: {"$oid": "..."}
    Returns ObjectId if valid, otherwise returns original value
    """
    if isinstance(value, dict):
        if '$oid' in value:
            try:
                return ObjectId(value['$oid'])
            except Exception as e:
                print(f"Error parsing ObjectId {value}: {e}")
                return value
    elif isinstance(value, str):
        try:
            return ObjectId(value)
        except:
            pass
    
    return value


def parse_mongo_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively parse MongoDB Extended JSON format in a document
    Handles dates, ObjectIds, and nested structures
    """
    if not isinstance(doc, dict):
        return doc
    
    parsed = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            # Check for MongoDB Extended JSON formats
            if '$date' in value:
                parsed[key] = parse_mongo_date(value)
            elif '$oid' in value:
                parsed[key] = parse_mongo_oid(value)
            elif isinstance(value, list):
                # Recursively parse list items
                parsed[key] = [parse_mongo_document(item) if isinstance(item, dict) else item for item in value]
            else:
                # Recursively parse nested dict
                parsed[key] = parse_mongo_document(value)
        elif isinstance(value, list):
            # Recursively parse list items
            parsed[key] = [parse_mongo_document(item) if isinstance(item, dict) else item for item in value]
        else:
            # Try to parse date strings
            if key in ['start_date', 'end_date', 'createdAt', 'created_at', 'updatedAt', 'updated_at', 'birthday', 'createdAt', 'pickedAt', 'deliveredAt', 'refund_at']:
                parsed[key] = parse_mongo_date(value)
            elif key in ['_id', 'userId', 'restaurantId', 'shipperId', 'paymentId', 'promoId', 'orderId', 'restaurant_id', 'user_id', 'shipper_id', 'payment_id', 'promo_id']:
                parsed[key] = parse_mongo_oid(value)
            else:
                parsed[key] = value
    
    return parsed

