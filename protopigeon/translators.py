import inspect
import copy
from protorpc import messages
from .converters import converters as default_converters


def _common_fields(entity, message, only=None, exclude=None):
    message_fields = [x.name for x in message.all_fields()]
    entity_properties = [x.name for x in entity.__table__.columns]

    if inspect.isclass(entity):
        fields = set(message_fields) & set(entity_properties)
    else:
        fields = set(message_fields)

    if only:
        fields = set(only) & set(fields)

    if exclude:
        fields = [x for x in fields if x not in exclude]

    return message_fields, fields


def to_message(entity, message, converters=None, only=None, exclude=None):
    message_fields, fields = _common_fields(entity, message, only, exclude)

    converters = dict(default_converters.items() + converters.items()) if converters else default_converters

    # Key first
    values = {}

    props = {x.name: (x.type, x.nullable) for x in entity.__table__.columns}

    # Other fields
    for field in fields:
        if field not in props:
            continue

        property = props[field]
        message_field = message.field_by_name(field)
        value = getattr(entity, field)

        converter = converters[property[0].__class__.__name__]

        if converter:
            if value is not None:  # only try to convert if the value is meaningful, otherwise leave it as Falsy.
                setattr(property[0], '_required', property[1])
                value = converter.to_message(entity, property[0], message_field, value)
            values[field] = value

    if inspect.isclass(message):
        return message(**values)
    else:
        for name, value in values.iteritems():
            setattr(message, name, value)
        return message


def to_entity(message, model, converters=None, only=None, exclude=None):
    message_fields, fields = _common_fields(model, message, only, exclude)

    converters = dict(default_converters.items() + converters.items()) if converters else default_converters

    values = {}

    props = {x.name: (x.type, x.nullable) for x in model.__table__.columns}

    # Other fields
    for field in fields:
        if field in props:
            property = props[field]
        else:
            continue

        converter = converters[property[0].__class__.__name__]
        message_field = message.field_by_name(field)
        value = getattr(message, field)

        if converter:
            if value is not None:
                setattr(property[0], '_required', property[1])
                value = converter.to_model(message, property[0], message_field, value)

            values[field] = value

    if inspect.isclass(model):
        return model(**values)
    else:
        model.populate(**values)
        return model


def model_message(Model, only=None, exclude=None, converters=None):
    class_name = Model.__name__ + 'Message'

    props = {x.name: (x.type, x.nullable) for x in Model.__table__.columns}
    field_names = [x.name for x in Model.__table__.columns]

    if exclude:
        field_names = [x for x in field_names if x not in exclude]

    if only:
        field_names = [x for x in field_names if x in only]

    converters = dict(default_converters.items() + converters.items()) if converters else default_converters

    field_dict = {}

    # Add all other fields.
    for count, name in enumerate(field_names, start=1):
        prop = props[name]
        converter = converters.get(prop[0].__class__.__name__, None)

        if converter:
            setattr(prop[0], '_required', prop[1])
            field_dict[name] = converter.to_field(Model, prop[0], count)

    return type(class_name, (messages.Message,), field_dict)


def list_message(message_type):
    name = message_type.__name__ + 'Collection'
    fields = {
        'items': messages.MessageField(message_type, 1, repeated=True),
        'nextPageToken': messages.StringField(2)
    }
    return type(name, (messages.Message,), fields)


collection_message = list_message


def compose(*args):
    fields = {}
    name = 'Composed'

    for message_cls in args:
        name += message_cls.__name__
        for field in message_cls.all_fields():
            fields[field.name] = field

    for n, orig_field in enumerate(fields.values(), 1):
        field = copy.copy(orig_field)
        # This is so ridiculously hacky. I'm not proud of it, but the alternative to doing this is trying to reconstruct each
        # field by figuring out the arguments originally passed into __init__. I think this is honestly a little cleaner.
        object.__setattr__(field, 'number', n)
        fields[field.name] = field

    return type(name, (messages.Message,), fields)
