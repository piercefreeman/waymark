from __future__ import annotations

import importlib
import json
import traceback
from typing import Any

from google.protobuf import json_format, struct_pb2
from pydantic import BaseModel

from proto import messages_pb2 as pb2

Struct = struct_pb2.Struct  # type: ignore[attr-defined]
Value = struct_pb2.Value  # type: ignore[attr-defined]
NULL_VALUE = struct_pb2.NULL_VALUE  # type: ignore[attr-defined]

PRIMITIVE_TYPES = (str, int, float, bool, type(None))


def dumps(value: Any) -> dict[str, Any]:
    """Serialize a Python value into the workflow argument schema."""

    message = _to_argument_value(value)
    return json_format.MessageToDict(message, preserving_proto_field_name=True)


def loads(data: Any) -> Any:
    """Deserialize a workflow argument payload into a Python object."""

    if isinstance(data, dict):
        argument = pb2.WorkflowArgumentValue()
        json_format.ParseDict(data, argument)
    elif isinstance(data, pb2.WorkflowArgumentValue):
        argument = data
    else:
        raise TypeError("argument value payload must be a dict or ArgumentValue message")
    return _from_argument_value(argument)


def dump_envelope(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def load_envelope(payload: bytes) -> Any:
    if isinstance(payload, bytes):
        text = payload.decode("utf-8")
    elif isinstance(payload, str):  # pragma: no cover - convenience path
        text = payload
    else:
        raise TypeError("payload must be bytes or str")
    return json.loads(text)


def _to_argument_value(value: Any) -> pb2.WorkflowArgumentValue:
    argument = pb2.WorkflowArgumentValue()
    if isinstance(value, PRIMITIVE_TYPES):
        argument.primitive.value.CopyFrom(_python_to_value(value))
        return argument
    if isinstance(value, BaseException):
        argument.exception.type = value.__class__.__name__
        argument.exception.module = value.__class__.__module__
        argument.exception.message = str(value)
        tb_text = "".join(traceback.format_exception(type(value), value, value.__traceback__))
        argument.exception.traceback = tb_text
        return argument
    if _is_base_model(value):
        model_class = value.__class__
        model_data = _serialize_model_data(value)
        argument.basemodel.module = model_class.__module__
        argument.basemodel.name = model_class.__qualname__
        struct = Struct()
        struct.update(model_data)
        argument.basemodel.data.CopyFrom(struct)
        return argument
    raise TypeError(f"unsupported value type {type(value)!r}")


def _from_argument_value(argument: pb2.WorkflowArgumentValue) -> Any:
    kind = argument.WhichOneof("kind")  # type: ignore[attr-defined]
    if kind == "primitive":
        return _value_to_python(argument.primitive.value)
    if kind == "basemodel":
        module = argument.basemodel.module
        name = argument.basemodel.name
        data = json_format.MessageToDict(argument.basemodel.data, preserving_proto_field_name=True)
        return _instantiate_serialized_model(module, name, data)
    if kind == "exception":
        return {
            "type": argument.exception.type,
            "module": argument.exception.module,
            "message": argument.exception.message,
            "traceback": argument.exception.traceback,
        }
    raise ValueError("argument value missing kind discriminator")


def _serialize_model_data(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="python")  # type: ignore[attr-defined]
    if hasattr(model, "dict"):
        return model.dict()  # type: ignore[attr-defined]
    return model.__dict__


def _python_to_value(value: Any) -> Value:
    message = Value()
    if value is None:
        message.null_value = NULL_VALUE
    elif isinstance(value, bool):
        message.bool_value = value
    elif isinstance(value, (int, float)):
        message.number_value = float(value)
    elif isinstance(value, str):
        message.string_value = value
    else:  # pragma: no cover - unreachable given PRIMITIVE_TYPES
        raise TypeError(f"unsupported primitive type {type(value)!r}")
    return message


def _value_to_python(value: Value) -> Any:
    kind = value.WhichOneof("kind")
    if kind == "null_value":
        return None
    if kind == "bool_value":
        return value.bool_value
    if kind == "number_value":
        return value.number_value
    if kind == "string_value":
        return value.string_value
    raise ValueError("unsupported primitive value kind")


def _instantiate_serialized_model(module: str, name: str, model_data: dict[str, Any]) -> Any:
    cls = _import_symbol(module, name)
    if hasattr(cls, "model_validate"):
        return cls.model_validate(model_data)  # type: ignore[attr-defined]
    return cls(**model_data)


def _is_base_model(value: Any) -> bool:
    return isinstance(value, BaseModel)


def _import_symbol(module: str, qualname: str) -> Any:
    module_obj = importlib.import_module(module)
    attr: Any = module_obj
    for part in qualname.split("."):
        attr = getattr(attr, part)
    if not isinstance(attr, type):
        raise ValueError(f"{qualname} from {module} is not a class")
    return attr
