from __future__ import annotations

import importlib
import traceback
from typing import Any

from google.protobuf import json_format, struct_pb2
from pydantic import BaseModel

from proto import messages_pb2 as pb2

Struct = struct_pb2.Struct  # type: ignore[attr-defined]
NULL_VALUE = struct_pb2.NULL_VALUE  # type: ignore[attr-defined]

PRIMITIVE_TYPES = (str, int, float, bool, type(None))


def dumps(value: Any) -> pb2.WorkflowArgumentValue:
    """Serialize a Python value into a WorkflowArgumentValue message."""

    return _to_argument_value(value)


def loads(data: Any) -> Any:
    """Deserialize a workflow argument payload into a Python object."""

    if isinstance(data, pb2.WorkflowArgumentValue):
        argument = data
    elif isinstance(data, dict):
        argument = pb2.WorkflowArgumentValue()
        json_format.ParseDict(data, argument)
    else:
        raise TypeError("argument value payload must be a dict or ArgumentValue message")
    return _from_argument_value(argument)


def build_arguments_from_kwargs(kwargs: dict[str, Any]) -> pb2.WorkflowArguments:
    arguments = pb2.WorkflowArguments()
    for key, value in kwargs.items():
        entry = arguments.arguments.add()
        entry.key = key
        entry.value.CopyFrom(dumps(value))
    return arguments


def arguments_to_kwargs(arguments: pb2.WorkflowArguments | None) -> dict[str, Any]:
    if arguments is None:
        return {}
    result: dict[str, Any] = {}
    for entry in arguments.arguments:
        result[entry.key] = loads(entry.value)
    return result


def _to_argument_value(value: Any) -> pb2.WorkflowArgumentValue:
    argument = pb2.WorkflowArgumentValue()
    if isinstance(value, PRIMITIVE_TYPES):
        argument.primitive.CopyFrom(_serialize_primitive(value))
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
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("workflow dict keys must be strings")
            entry = argument.dict_value.entries.add()
            entry.key = key
            entry.value.CopyFrom(_to_argument_value(item))
        return argument
    if isinstance(value, list):
        for item in value:
            item_value = argument.list_value.items.add()
            item_value.CopyFrom(_to_argument_value(item))
        return argument
    if isinstance(value, tuple):
        for item in value:
            item_value = argument.tuple_value.items.add()
            item_value.CopyFrom(_to_argument_value(item))
        return argument
    raise TypeError(f"unsupported value type {type(value)!r}")


def _from_argument_value(argument: pb2.WorkflowArgumentValue) -> Any:
    kind = argument.WhichOneof("kind")  # type: ignore[attr-defined]
    if kind == "primitive":
        return _primitive_to_python(argument.primitive)
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
        return model.model_dump(mode="python")  # type: ignore[attr-defined]
    if hasattr(model, "dict"):
        return model.dict()  # type: ignore[attr-defined]
    return model.__dict__


def _serialize_primitive(value: Any) -> pb2.PrimitiveWorkflowArgument:
    primitive = pb2.PrimitiveWorkflowArgument()
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


def _primitive_to_python(primitive: pb2.PrimitiveWorkflowArgument) -> Any:
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
