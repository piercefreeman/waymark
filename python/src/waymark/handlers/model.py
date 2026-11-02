"""Data constructors whose complete behavior can be represented as an IR dictionary."""

import ast
import dataclasses
import inspect
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel
from pydantic_core import PydanticUndefined

from waymark.call_binding import bind_call
from waymark.compiler_context import CompilerContext
from waymark.compiler_literals import literal_expression
from waymark.compiler_types import has_open_type_parameters
from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class ModelArguments:
    fields: dict[str, ast.expr]


class ModelHandler(LanguageCallHandler[ModelArguments]):
    arguments_type = ModelArguments
    requires_await = False
    recommendation = (
        "Construct models with factories, validation, or custom hooks inside an action."
    )

    def __init__(self, model: type):
        self.model = model
        self.name = model.__name__

    def parse_arguments(self, node: ast.Call) -> ModelArguments:
        """Reject unrepresentable construction behavior before binding fields and defaults."""
        parameters: list[inspect.Parameter] = []
        if has_open_type_parameters(self.model):
            raise self.error(node, "open generic types are unsupported")
        if issubclass(self.model, BaseModel):
            if (
                type(self.model) is not type(BaseModel)
                or self.model.__new__ is not BaseModel.__new__
                or self.model.__setattr__ is not BaseModel.__setattr__
                or self.model.__getattribute__ is not BaseModel.__getattribute__
            ):
                raise self.error(node, "custom construction or attribute hooks are unsupported")
            decorators = self.model.__pydantic_decorators__
            if (
                any(
                    (
                        decorators.validators,
                        decorators.root_validators,
                        decorators.field_validators,
                        decorators.model_validators,
                        decorators.field_serializers,
                        decorators.model_serializers,
                        decorators.computed_fields,
                    )
                )
                or self.model.__pydantic_custom_init__
                or self.model.model_post_init is not BaseModel.model_post_init
            ):
                raise self.error(
                    node,
                    "custom validators, serializers, initializers, and computed fields are unsupported",
                )
            if self.model.model_config or self.model.__private_attributes__:
                raise self.error(
                    node, "custom model configuration and private attributes are unsupported"
                )
            for name, field in self.model.model_fields.items():
                if (
                    field.default_factory is not None
                    or field.metadata
                    or field.alias is not None
                    or field.validation_alias is not None
                    or has_open_type_parameters(field.annotation)
                ):
                    raise self.error(
                        node,
                        f"field {name!r} has an unsupported factory, alias, annotation, or validation rule",
                    )
                default = (
                    inspect.Parameter.empty if field.default is PydanticUndefined else field.default
                )
                parameters.append(
                    inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, default=default)
                )
        else:
            if (
                type(self.model) is not type
                or self.model.__new__ is not object.__new__
                or self.model.__getattribute__ is not object.__getattribute__
            ):
                raise self.error(node, "custom construction or attribute hooks are unsupported")
            dataclass_parameters = next(
                vars(base)["__dataclass_params__"]
                for base in self.model.__mro__
                if "__dataclass_params__" in vars(base)
            )
            setter = self.model.__setattr__
            if setter is not object.__setattr__ and not (
                dataclass_parameters.frozen
                and inspect.isfunction(setter)
                and setter.__code__.co_filename == "<string>"
            ):
                raise self.error(node, "custom __setattr__ hooks are unsupported")
            if any("__getattr__" in vars(base) for base in self.model.__mro__):
                raise self.error(node, "custom attribute hooks are unsupported")
            if any("__post_init__" in vars(base) for base in self.model.__mro__):
                raise self.error(node, "custom __post_init__ hooks are unsupported")
            initializer = self.model.__init__
            if (
                not inspect.isfunction(initializer)
                or initializer.__code__.co_filename != "<string>"
            ):
                raise self.error(node, "custom or inherited dataclass initializers are unsupported")
            fields = dataclasses.fields(self.model)
            for field in fields:
                for base in self.model.__mro__:
                    if field.name in vars(base):
                        default = vars(base)[field.name]
                        if any(
                            "__get__" in vars(owner) or "__set__" in vars(owner)
                            for owner in type(default).__mro__
                        ):
                            raise self.error(node, "descriptor-backed fields are unsupported")
                        break
            if any(
                not field.init
                or field.default_factory is not dataclasses.MISSING
                or has_open_type_parameters(field.type)
                for field in fields
            ):
                raise self.error(
                    node,
                    "dataclass factories, init=False fields, and open generic fields are unsupported",
                )
            signature = inspect.signature(self.model)
            if set(signature.parameters) != {field.name for field in fields}:
                raise self.error(node, "InitVar and inherited constructor layouts are unsupported")
            parameters = list(signature.parameters.values())
        bound = bind_call(node, inspect.Signature(parameters))
        fields = dict(bound.arguments)
        for parameter in parameters:
            if parameter.name not in fields and parameter.default is not inspect.Parameter.empty:
                fields[parameter.name] = literal_expression(parameter.default, node)
        return ModelArguments(fields)

    def lower(
        self, arguments: ModelArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Represent the validated constructor as a dictionary in argument evaluation order."""
        entries = []
        for name, argument in arguments.fields.items():
            value = context.expression(argument)
            assert value is not None
            entries.append(
                ir.DictEntry(key=ir.Expr(literal=ir.Literal(string_value=name)), value=value)
            )
        return LoweredCall(ir.Expr(dict=ir.DictExpr(entries=entries)))


def model_handler(value: Any) -> ModelHandler | None:
    """Recognize data-class identities; eligibility is checked when their calls are bound."""
    if isinstance(value, type) and (
        issubclass(value, BaseModel) or dataclasses.is_dataclass(value)
    ):
        return ModelHandler(value)
    return None
