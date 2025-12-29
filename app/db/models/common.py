from bson import ObjectId


class PyObjectId(ObjectId):
    """Validator cho ObjectId của MongoDB dùng trong Pydantic."""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, info):  # type: ignore[override]
        if v is None:
            return None
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):  # type: ignore[override]
        field_schema.update(type="string")

