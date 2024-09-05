# import sys
import json
from datetime import datetime

from peewee import(
    Model,
    CharField,
    IntegerField,
    BigIntegerField,
    TextField,
    CompositeKey,
    BigAutoField,
    BooleanField,
    UUIDField,
    DateTimeField,
    DateField,
    ForeignKeyField,
    IPField,

    SqliteDatabase
)

DATABASE = SqliteDatabase(database='database.sqlite3')

DATABASE.connect()

class UpdateDateTimeField(DateTimeField):
    def db_value(self, value):
        value = datetime.now()
        return super().db_value(value)


class JSONField(TextField):
    def db_value(self, value):
        if value is None:
            value = {}
        return json.dumps(value)

    def python_value(self, value):
        if value is None:
            return {}
        return json.loads(value)


class AbstractModel(Model):
    created_date = DateTimeField(default=datetime.now)
    updated_date = UpdateDateTimeField(default=datetime.now)
    manager = DATABASE

    class Meta:
        database = DATABASE


################################################################################
#                        models
################################################################################
class Category(AbstractModel):
    name = CharField(max_length=16, unique=True)
    # appsecret = UUIDField(default=uuid4)
    parent = ForeignKeyField('self', null=True, backref='children')


class Video(AbstractModel):
    name = CharField(max_length=32)
    category = ForeignKeyField(Category, backref='video')
    area = CharField(max_length=16)
    year = DateField()
    unique_key = CharField(max_length=64, unique=True)


class PlatformVideo(AbstractModel):
    video = ForeignKeyField(Video, backref='platform_video')
    platform = CharField(max_length=32)
    resources = JSONField()


Category.create_table()
Video.create_table()
PlatformVideo.create_table()