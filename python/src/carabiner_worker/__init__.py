"""Public API for user-defined carabiner actions."""

from .registry import action, registry
from .serialization import (
    ActionCall,
    ActionResultPayload,
    deserialize_action_call,
    deserialize_result_payload,
    serialize_action_call,
    serialize_error_payload,
    serialize_result_payload,
)

__all__ = [
    "action",
    "registry",
    "ActionCall",
    "ActionResultPayload",
    "serialize_action_call",
    "deserialize_action_call",
    "serialize_result_payload",
    "deserialize_result_payload",
    "serialize_error_payload",
]
