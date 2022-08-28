from mimesis import Person
from mimesis.locales import Locale
from mimesis.schema import Field, Schema

_ = Field(locale=Locale.EN)
user_schema = Schema(schema=lambda: {
    "id": _("increment"),
    "name": _("full_name"),
    "email": _("email"),
    "password": _("password"),
    "registration_dt": _("timestamp"),
    "last_login_dt": _("timestamp"),
    "is_admin": _("boolean")
})
user_schema.to_csv(file_path='user_data.csv', iterations=10)


list_schema = Schema(schema=lambda: {
    "id": _("increment"),
    "title": _("full_name"),
    "created_at": _("timestamp"),
    "creator_id": _("id")
})  
list_schema.to_csv(file_path='list_data.csv', iterations=10)
