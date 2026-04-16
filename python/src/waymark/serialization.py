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
from waymark.type_coercion import instantiate_typed_model

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
    normalized = _normalize_value(value)
    primitive = _serialize_scalar_value(normalized)
    argument = pb2.WorkflowArgumentValue()
    if primitive is not None:
        argument.primitive.CopyFrom(primitive)
        return argument
    argument.flat_value.CopyFrom(_serialize_flat_argument(normalized))
    return argument


def _from_argument_value(argument: pb2.WorkflowArgumentValue) -> Any:
    kind = argument.WhichOneof("kind")  # type: ignore[attr-defined]
    if kind == "primitive":
        return _primitive_to_python(argument.primitive)
    if kind == "flat_value":
        return _deserialize_flat_argument(argument.flat_value)
    if kind == "basemodel":
        module = argument.basemodel.module
        name = argument.basemodel.name
        # Deserialize dict entries to preserve types
        data: dict[str, Any] = {}
        for entry in argument.basemodel.data.entries:
            data[entry.key] = _from_argument_value(entry.value)
        return _instantiate_serialized_model(module, name, data)
    if kind == "exception":
        values: dict[str, Any] = {}
        if argument.exception.HasField("values"):
            for entry in argument.exception.values.entries:
                values[entry.key] = _from_argument_value(entry.value)
        return {
            "type": argument.exception.type,
            "module": argument.exception.module,
            "message": argument.exception.message,
            "traceback": argument.exception.traceback,
            "values": values,
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
        return model.model_dump(mode="json")  # type: ignore[attr-defined]
    if hasattr(model, "dict"):
        return model.dict()  # type: ignore[attr-defined]
    return model.__dict__


def _serialize_exception_values(exc: BaseException) -> dict[str, Any]:
    values = dict(vars(exc))
    if "args" not in values:
        values["args"] = exc.args
    return values


def _normalize_value(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return _normalize_value(value.value)
    if isinstance(value, bytes):
        return b64encode(value).decode("ascii")
    if isinstance(value, PurePath):
        return str(value)
    return value


def _serialize_scalar_value(value: Any) -> pb2.PrimitiveWorkflowArgument | None:
    if isinstance(value, PRIMITIVE_TYPES):
        return _serialize_primitive(value)
    return None


def _serialize_flat_argument(value: Any) -> pb2.FlatWorkflowArgument:
    flat = pb2.FlatWorkflowArgument()
    active_ids: set[int] = set()
    flat.root_node_id = _append_flat_node(flat, value, active_ids)
    return flat


def _append_flat_node(
    flat: pb2.FlatWorkflowArgument,
    value: Any,
    active_ids: set[int],
) -> int:
    normalized = _normalize_value(value)
    primitive = _serialize_scalar_value(normalized)
    if primitive is not None:
        node = flat.nodes.add()
        node.node_id = len(flat.nodes) - 1
        node.primitive.CopyFrom(primitive)
        return node.node_id

    cycle_id = _cycle_id(normalized)
    if cycle_id is not None:
        if cycle_id in active_ids:
            raise TypeError("cyclic workflow values are not supported")
        active_ids.add(cycle_id)

    try:
        if isinstance(normalized, (set, frozenset)):
            item_ids = [_append_flat_node(flat, item, active_ids) for item in normalized]
            node = flat.nodes.add()
            node.node_id = len(flat.nodes) - 1
            node.list_value.item_node_ids.extend(item_ids)
            return node.node_id

        if isinstance(normalized, BaseException):
            node = flat.nodes.add()
            node.node_id = len(flat.nodes) - 1
            node.exception.type = normalized.__class__.__name__
            node.exception.module = normalized.__class__.__module__
            node.exception.message = str(normalized)
            node.exception.traceback = "".join(
                traceback.format_exception(type(normalized), normalized, normalized.__traceback__)
            )
            for cls in normalized.__class__.__mro__:
                if cls is object:
                    continue
                node.exception.type_hierarchy.append(cls.__name__)
            for key, item in _serialize_exception_values(normalized).items():
                entry = node.exception.value_entries.add()
                entry.key = key
                try:
                    entry.value_node_id = _append_flat_node(flat, item, active_ids)
                except TypeError:
                    entry.value_node_id = _append_flat_node(flat, str(item), active_ids)
            return node.node_id

        if _is_base_model(normalized):
            model_class = normalized.__class__
            node = flat.nodes.add()
            node.node_id = len(flat.nodes) - 1
            node.basemodel.module = model_class.__module__
            node.basemodel.name = model_class.__qualname__
            for key, item in _serialize_model_data(normalized).items():
                entry = node.basemodel.data_entries.add()
                entry.key = key
                entry.value_node_id = _append_flat_node(flat, item, active_ids)
            return node.node_id

        if _is_dataclass_instance(normalized):
            dc_class = normalized.__class__
            node = flat.nodes.add()
            node.node_id = len(flat.nodes) - 1
            node.basemodel.module = dc_class.__module__
            node.basemodel.name = dc_class.__qualname__
            for key, item in dataclasses.asdict(normalized).items():
                entry = node.basemodel.data_entries.add()
                entry.key = key
                entry.value_node_id = _append_flat_node(flat, item, active_ids)
            return node.node_id

        if isinstance(normalized, dict):
            node = flat.nodes.add()
            node.node_id = len(flat.nodes) - 1
            for key, item in normalized.items():
                if not isinstance(key, str):
                    raise TypeError("workflow dict keys must be strings")
                entry = node.dict_value.entries.add()
                entry.key = key
                entry.value_node_id = _append_flat_node(flat, item, active_ids)
            return node.node_id

        if isinstance(normalized, list):
            item_ids = [_append_flat_node(flat, item, active_ids) for item in normalized]
            node = flat.nodes.add()
            node.node_id = len(flat.nodes) - 1
            node.list_value.item_node_ids.extend(item_ids)
            return node.node_id

        if isinstance(normalized, tuple):
            item_ids = [_append_flat_node(flat, item, active_ids) for item in normalized]
            node = flat.nodes.add()
            node.node_id = len(flat.nodes) - 1
            node.tuple_value.item_node_ids.extend(item_ids)
            return node.node_id

        raise TypeError(f"unsupported value type {type(normalized)!r}")
    finally:
        if cycle_id is not None:
            active_ids.remove(cycle_id)


def _cycle_id(value: Any) -> int | None:
    if isinstance(
        value,
        (BaseException, BaseModel, dict, list, tuple, set, frozenset),
    ) or _is_dataclass_instance(value):
        return id(value)
    return None


def _deserialize_flat_argument(flat: pb2.FlatWorkflowArgument) -> Any:
    if not flat.nodes:
        raise ValueError("flat workflow argument missing nodes")

    nodes_by_id: dict[int, pb2.FlatWorkflowNode] = {}
    for node in flat.nodes:
        if node.node_id in nodes_by_id:
            raise ValueError(f"duplicate flat workflow node id {node.node_id}")
        nodes_by_id[node.node_id] = node

    cache: dict[int, Any] = {}
    return _deserialize_flat_node(flat.root_node_id, nodes_by_id, cache)


def _deserialize_flat_node(
    node_id: int,
    nodes_by_id: dict[int, pb2.FlatWorkflowNode],
    cache: dict[int, Any],
) -> Any:
    if node_id in cache:
        return cache[node_id]

    node = nodes_by_id.get(node_id)
    if node is None:
        raise ValueError(f"flat workflow node {node_id} not found")

    kind = node.WhichOneof("kind")  # type: ignore[attr-defined]
    if kind == "primitive":
        value = _primitive_to_python(node.primitive)
    elif kind == "list_value":
        value = [
            _deserialize_flat_node(item_id, nodes_by_id, cache)
            for item_id in node.list_value.item_node_ids
        ]
    elif kind == "tuple_value":
        value = tuple(
            _deserialize_flat_node(item_id, nodes_by_id, cache)
            for item_id in node.tuple_value.item_node_ids
        )
    elif kind == "dict_value":
        value = {
            entry.key: _deserialize_flat_node(entry.value_node_id, nodes_by_id, cache)
            for entry in node.dict_value.entries
        }
    elif kind == "basemodel":
        data = {
            entry.key: _deserialize_flat_node(entry.value_node_id, nodes_by_id, cache)
            for entry in node.basemodel.data_entries
        }
        value = _instantiate_serialized_model(node.basemodel.module, node.basemodel.name, data)
    elif kind == "exception":
        values = {
            entry.key: _deserialize_flat_node(entry.value_node_id, nodes_by_id, cache)
            for entry in node.exception.value_entries
        }
        value = {
            "type": node.exception.type,
            "module": node.exception.module,
            "message": node.exception.message,
            "traceback": node.exception.traceback,
            "values": values,
        }
    else:
        raise ValueError("flat workflow node missing kind discriminator")

    cache[node_id] = value
    return value


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
