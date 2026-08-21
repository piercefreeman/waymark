import dataclasses
import importlib
import traceback
from base64 import b64encode
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Any
from uuid import UUID

from google.protobuf import json_format, struct_pb2
from pydantic import BaseModel

from waymark.proto import messages_pb2 as pb2
from waymark.proto import python_value_pb2 as pb2v
from waymark.type_coercion import instantiate_typed_model

NULL_VALUE = struct_pb2.NULL_VALUE  # type: ignore[attr-defined]

PRIMITIVE_TYPES = (str, int, float, bool, type(None))


def dumps(value: Any) -> pb2v.Value:
    """Serialize a Python value into a Value message."""

    return _to_argument_value(value)


def dumps_exception(exc: BaseException) -> pb2v.ExceptionValue:
    """Serialize an exception into the exception value it denotes."""

    return pb2v.ExceptionValue(
        type_id=exc.__class__.__name__,
        details=_exception_details(exc),
    )


def loads(data: Any) -> Any:
    """Deserialize a workflow argument payload into a Python object."""

    if isinstance(data, pb2v.Value):
        argument = data
    elif isinstance(data, bytes):
        # A framing-level argument value: an encoded
        # Value.
        argument = pb2v.Value.FromString(data)
    elif isinstance(data, dict):
        argument = pb2v.Value()
        json_format.ParseDict(data, argument)
    else:
        raise TypeError("argument value payload must be bytes, a dict or ArgumentValue message")
    return _from_argument_value(argument)


def build_arguments_from_kwargs(kwargs: dict[str, Any]) -> pb2.WorkflowArguments:
    arguments = pb2.WorkflowArguments()
    for key, value in kwargs.items():
        entry = arguments.arguments.add()
        entry.key = key
        entry.value = dumps(value).SerializeToString()
    return arguments


def action_arguments_to_kwargs(payload: bytes) -> dict[str, Any]:
    """Decode an encoded ActionArguments payload into call kwargs.

    The bytes carry this language's own arguments message; the entries
    hold values directly. Empty bytes mean no arguments.
    """
    if not payload:
        return {}
    message = pb2v.ActionArguments.FromString(payload)
    return {entry.key: loads(entry.value) for entry in message.arguments}


def arguments_to_kwargs(arguments: pb2.WorkflowArguments | None) -> dict[str, Any]:
    if arguments is None:
        return {}
    result: dict[str, Any] = {}
    for entry in arguments.arguments:
        result[entry.key] = loads(entry.value)
    return result


def _to_argument_value(value: Any) -> pb2v.Value:
    argument = pb2v.Value()
    if isinstance(value, PRIMITIVE_TYPES):
        argument.primitive.CopyFrom(_serialize_primitive(value))
        return argument
    if isinstance(value, UUID):
        # Serialize UUID as string primitive
        argument.primitive.CopyFrom(_serialize_primitive(str(value)))
        return argument
    if isinstance(value, datetime):
        # Serialize datetime as ISO format string
        argument.primitive.CopyFrom(_serialize_primitive(value.isoformat()))
        return argument
    if isinstance(value, date):
        # Serialize date as ISO format string (must come after datetime check)
        argument.primitive.CopyFrom(_serialize_primitive(value.isoformat()))
        return argument
    if isinstance(value, time):
        # Serialize time as ISO format string
        argument.primitive.CopyFrom(_serialize_primitive(value.isoformat()))
        return argument
    if isinstance(value, timedelta):
        # Serialize timedelta as total seconds
        argument.primitive.CopyFrom(_serialize_primitive(value.total_seconds()))
        return argument
    if isinstance(value, Decimal):
        # Serialize Decimal as string to preserve precision
        argument.primitive.CopyFrom(_serialize_primitive(str(value)))
        return argument
    if isinstance(value, Enum):
        # Serialize Enum as its value
        return _to_argument_value(value.value)
    if isinstance(value, bytes):
        # Serialize bytes as base64 string
        argument.primitive.CopyFrom(_serialize_primitive(b64encode(value).decode("ascii")))
        return argument
    if isinstance(value, PurePath):
        # Serialize Path as string
        argument.primitive.CopyFrom(_serialize_primitive(str(value)))
        return argument
    if isinstance(value, (set, frozenset)):
        # Serialize sets as lists
        argument.list_value.SetInParent()
        for item in value:
            item_value = argument.list_value.items.add()
            item_value.CopyFrom(_to_argument_value(item))
        return argument
    if isinstance(value, BaseException):
        argument.exception.type_id = value.__class__.__name__
        argument.exception.details.CopyFrom(_exception_details(value))
        return argument
    if _is_base_model(value):
        model_class = value.__class__
        model_data = _serialize_model_data(value)
        argument.basemodel.module = model_class.__module__
        argument.basemodel.name = model_class.__qualname__
        # Serialize as dict to preserve types (Struct converts all numbers to float)
        for key, item in model_data.items():
            entry = argument.basemodel.data.entries.add()
            entry.key = key
            entry.value.CopyFrom(_to_argument_value(item))
        return argument
    if _is_dataclass_instance(value):
        # Dataclasses use the same basemodel serialization path as Pydantic models
        dc_class = value.__class__
        dc_data = dataclasses.asdict(value)
        argument.basemodel.module = dc_class.__module__
        argument.basemodel.name = dc_class.__qualname__
        for key, item in dc_data.items():
            entry = argument.basemodel.data.entries.add()
            entry.key = key
            entry.value.CopyFrom(_to_argument_value(item))
        return argument
    if isinstance(value, dict):
        argument.dict_value.SetInParent()
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("workflow dict keys must be strings")
            entry = argument.dict_value.entries.add()
            entry.key = key
            entry.value.CopyFrom(_to_argument_value(item))
        return argument
    if isinstance(value, list):
        argument.list_value.SetInParent()
        for item in value:
            item_value = argument.list_value.items.add()
            item_value.CopyFrom(_to_argument_value(item))
        return argument
    if isinstance(value, tuple):
        argument.tuple_value.SetInParent()
        for item in value:
            item_value = argument.tuple_value.items.add()
            item_value.CopyFrom(_to_argument_value(item))
        return argument
    raise TypeError(f"unsupported value type {type(value)!r}")


def _exception_details(value: BaseException) -> pb2v.Value:
    """Build the details value carried alongside an exception's type id.

    The particulars are this language's own choice, so they ride as an
    ordinary dict rather than as wire-level structure: the message, the
    defining module, the traceback, the class hierarchy `except` matches
    on, and whatever values the exception itself carries.
    """
    # Include the exception class hierarchy (MRO) for proper except matching.
    # This allows `except LookupError:` to catch KeyError, etc.
    hierarchy = [cls.__name__ for cls in value.__class__.__mro__ if cls is not object]

    values: dict[str, Any] = {}
    for key, item in _serialize_exception_values(value).items():
        try:
            _to_argument_value(item)
        except TypeError:
            item = str(item)
        values[key] = item

    return _to_argument_value(
        {
            "message": str(value),
            "module": value.__class__.__module__,
            "traceback": "".join(
                traceback.format_exception(type(value), value, value.__traceback__)
            ),
            "type_hierarchy": hierarchy,
            "values": values,
        }
    )


def _from_argument_value(argument: pb2v.Value) -> Any:
    kind = argument.WhichOneof("kind")  # type: ignore[attr-defined]
    if kind == "primitive":
        return _primitive_to_python(argument.primitive)
    if kind == "basemodel":
        module = argument.basemodel.module
        name = argument.basemodel.name
        # Deserialize dict entries to preserve types
        data: dict[str, Any] = {}
        for entry in argument.basemodel.data.entries:
            data[entry.key] = _from_argument_value(entry.value)
        return _instantiate_serialized_model(module, name, data)
    if kind == "exception":
        details = (
            _from_argument_value(argument.exception.details)
            if argument.exception.HasField("details")
            else {}
        )
        return {"type_id": argument.exception.type_id, "details": details}
    if kind == "list_value":
        return [_from_argument_value(item) for item in argument.list_value.items]
    if kind == "tuple_value":
        return tuple(_from_argument_value(item) for item in argument.tuple_value.items)
    if kind == "dict_value":
        result: dict[str, Any] = {}
        for entry in argument.dict_value.entries:
            result[entry.key] = _from_argument_value(entry.value)
        return result
    raise ValueError("argument value missing kind discriminator")


def _serialize_model_data(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")  # type: ignore[attr-defined]
    if hasattr(model, "dict"):
        return model.dict()  # type: ignore[attr-defined]
    return model.__dict__


def _serialize_exception_values(exc: BaseException) -> dict[str, Any]:
    values = dict(vars(exc))
    if "args" not in values:
        values["args"] = exc.args
    return values


def _serialize_primitive(value: Any) -> pb2v.PrimitiveValue:
    primitive = pb2v.PrimitiveValue()
    if value is None:
        primitive.null_value = NULL_VALUE
    elif isinstance(value, bool):
        primitive.bool_value = value
    elif isinstance(value, int) and not isinstance(value, bool):
        primitive.int_value = value
    elif isinstance(value, float):
        primitive.double_value = value
    elif isinstance(value, str):
        primitive.string_value = value
    else:  # pragma: no cover - unreachable given PRIMITIVE_TYPES
        raise TypeError(f"unsupported primitive type {type(value)!r}")
    return primitive


def _primitive_to_python(primitive: pb2v.PrimitiveValue) -> Any:
    kind = primitive.WhichOneof("kind")  # type: ignore[attr-defined]
    if kind == "string_value":
        return primitive.string_value
    if kind == "double_value":
        return primitive.double_value
    if kind == "int_value":
        return primitive.int_value
    if kind == "bool_value":
        return primitive.bool_value
    if kind == "null_value":
        return None
    raise ValueError("primitive argument missing kind discriminator")


def _instantiate_serialized_model(module: str, name: str, model_data: dict[str, Any]) -> Any:
    cls = _import_symbol(module, name)
    return instantiate_typed_model(cls, model_data)


def _is_base_model(value: Any) -> bool:
    return isinstance(value, BaseModel)


def _is_dataclass_instance(value: Any) -> bool:
    """Check if value is a dataclass instance (not a class)."""
    return dataclasses.is_dataclass(value) and not isinstance(value, type)


def _import_symbol(module: str, qualname: str) -> Any:
    module_obj = importlib.import_module(module)
    attr: Any = module_obj
    for part in qualname.split("."):
        attr = getattr(attr, part)
    if not isinstance(attr, type):
        raise ValueError(f"{qualname} from {module} is not a class")
    return attr
