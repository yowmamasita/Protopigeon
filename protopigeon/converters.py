import datetime
from protorpc import messages, message_types, util
from .types import DateMessage, TimeMessage


class Converter(object):
    @staticmethod
    def to_message(Mode, property, field, value):
        return value

    @staticmethod
    def to_model(Message, property, field, value):
        return value

    @staticmethod
    def to_field(Model, property, count):
        return None


class StringConverter(Converter):
    @staticmethod
    def to_field(Model, property, count):
        return messages.StringField(count, required=property._required)


class BytesConverter(Converter):
    @staticmethod
    def to_field(Model, property, count):
        return messages.BytesField(count)


class BooleanConverter(Converter):
    @staticmethod
    def to_field(Model, property, count):
        return messages.BooleanField(count)


class IntegerConverter(Converter):
    @staticmethod
    def to_field(Model, property, count):
        return messages.IntegerField(count)


class FloatConverter(Converter):
    @staticmethod
    def to_field(Model, property, count):
        return messages.FloatField(count)


class DateTimeConverter(Converter):
    @staticmethod
    def to_field(Model, property, count):
        return message_types.DateTimeField(count)


class DateConverter(Converter):
    @staticmethod
    def to_message(Mode, property, field, value):
        return DateMessage(
            year=value.year,
            month=value.month,
            day=value.day)

    @staticmethod
    def to_model(Message, property, field, value):
        return datetime.date(value.year, value.month, value.day)

    @staticmethod
    def to_field(Model, property, count):
        return messages.MessageField(DateMessage, count)


class TimeConverter(Converter):
    @staticmethod
    def to_message(Mode, property, field, value):
        time_zone_offset = 0
        if value.tzinfo is not None:
            utc_offset = value.tzinfo.utcoffset(value)
            if utc_offset is not None:
                time_zone_offset = int(utc_offset.total_seconds() / 60)

        return TimeMessage(
            hour=value.hour,
            minute=value.minute,
            second=value.second,
            microsecond=value.microsecond,
            time_zone_offset=time_zone_offset)

    @staticmethod
    def to_model(Message, property, field, value):
        timezone = None
        if value.time_zone_offset:
            timezone = util.TimeZoneOffset(value.time_zone_offset)
        return datetime.time(value.hour, value.minute, value.second, value.microsecond, timezone)

    @staticmethod
    def to_field(Model, property, count):
        return messages.MessageField(TimeMessage, count)


converters = {
    'BigInteger': IntegerConverter,
    'Boolean': BooleanConverter,
    'Date': DateConverter,
    'DateTime': DateTimeConverter,
    'Enum': StringConverter,
    'Float': FloatConverter,
    'Integer': IntegerConverter,
    'Interval': StringConverter,
    'LargeBinary': BytesConverter,
    'Numeric': IntegerConverter,
    'PickleType': BytesConverter,
    'SmallInteger': IntegerConverter,
    'String': StringConverter,
    'Text': StringConverter,
    'Time': TimeConverter,
    'Unicode': StringConverter,
    'UnicodeText': StringConverter,
}
