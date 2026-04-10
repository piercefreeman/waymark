import dataclasses
from base64 import b64decode
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import PurePath
from types import UnionType
from typing import Any, Union, cast, get_args, get_origin, get_type_hints
from uuid import UUID

from pydantic import BaseModel

COERCIBLE_TYPES = (UUID, datetime, date, time, timedelta, Decimal, bytes, PurePath)


def is_pydantic_model_type(target_type: Any) -> bool:
    try:
        return isinstance(target_type, type) and issubclass(target_type, BaseModel)
    except TypeError:
        return False


def is_dataclass_type(target_type: Any) -> bool:
    return isinstance(target_type, type) and dataclasses.is_dataclass(target_type)


def instantiate_typed_model(target_type: type, value: dict[str, Any]) -> Any:
    if is_pydantic_model_type(target_type):
        model_type = cast(type[BaseModel], target_type)
        return model_type.model_validate(value)
    if is_dataclass_type(target_type):
        return _coerce_dict_to_dataclass(value, target_type)
    return target_type(**value)


def coerce_value(value: Any, target_type: type) -> Any:
    if value is None or target_type is Any:
        return value

    origin = get_origin(target_type)
    if origin is UnionType or origin is Union:
        return _coerce_union_value(value, target_type)

    if isinstance(target_type, type) and issubclass(target_type, COERCIBLE_TYPES):
        return _coerce_primitive(value, target_type)

    if isinstance(value, dict) and (
        is_pydantic_model_type(target_type) or is_dataclass_type(target_type)
    ):
        return instantiate_typed_model(target_type, value)

    if origin is None:
        return value

    args = get_args(target_type)

    if origin is list and isinstance(value, list) and args:
        item_type = args[0]
        return [coerce_value(item, item_type) for item in value]

    if origin is set and isinstance(value, list) and args:
        item_type = args[0]
        return {coerce_value(item, item_type) for item in value}

    if origin is frozenset and isinstance(value, list) and args:
        item_type = args[0]
        return frozenset(coerce_value(item, item_type) for item in value)

    if origin is tuple and isinstance(value, (list, tuple)) and args:
        if len(args) == 2 and args[1] is ...:
            item_type = args[0]
            return tuple(coerce_value(item, item_type) for item in value)
        return tuple(
            coerce_value(item, item_type) for item, item_type in zip(value, args, strict=False)
        )

    if origin is dict and isinstance(value, dict) and len(args) == 2:
        key_type, value_type = args
        return {
            coerce_value(key, key_type): coerce_value(item, value_type)
            for key, item in value.items()
        }

    return value


def _coerce_union_value(value: Any, target_type: type) -> Any:
    for union_type in get_args(target_type):
        if union_type is type(None):
            if value is None:
                return None
            continue
        try:
            coerced = coerce_value(value, union_type)
        except Exception:
            continue
        if coerced is not value:
            return coerced
        if isinstance(union_type, type) and isinstance(value, union_type):
            return value
    return value


def _coerce_primitive(value: Any, target_type: type) -> Any:
    if target_type is UUID:
        if isinstance(value, UUID):
            return value
        if isinstance(value, str):
            return UUID(value)
        return value

    if target_type is datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value

    if target_type is date:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        return value

    if target_type is time:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            return time.fromisoformat(value)
        return value

    if target_type is timedelta:
        if isinstance(value, timedelta):
            return value
        if isinstance(value, (int, float)):
            return timedelta(seconds=value)
        return value

    if target_type is Decimal:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (str, int, float)):
            return Decimal(str(value))
        return value

    if target_type is bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return b64decode(value)
        return value

    if issubclass(target_type, PurePath):
        if isinstance(value, PurePath):
            if isinstance(value, target_type):
                return value
            return target_type(str(value))
        if isinstance(value, str):
            return target_type(value)
        return value

    return value


def _coerce_dict_to_dataclass(value: dict[str, Any], target_type: type) -> Any:
    try:
        field_types = get_type_hints(target_type)
    except Exception:
        field_types = {}

    init_values: dict[str, Any] = {}
    deferred_values: dict[str, Any] = {}
    field_names: set[str] = set()

    for field in dataclasses.fields(target_type):
        field_names.add(field.name)
        if field.name not in value:
            continue

        field_value = value[field.name]
        if field.name in field_types:
            field_value = coerce_value(field_value, field_types[field.name])

        if field.init:
            init_values[field.name] = field_value
        else:
            deferred_values[field.name] = field_value

    extra_fields = set(value) - field_names
    if extra_fields:
        extras = ", ".join(sorted(extra_fields))
        raise TypeError(f"{target_type.__qualname__} got unexpected field(s): {extras}")

    instance = target_type(**init_values)
    for field_name, field_value in deferred_values.items():
        object.__setattr__(instance, field_name, field_value)
    return instance
