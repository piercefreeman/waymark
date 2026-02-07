from __future__ import annotations

import inspect
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Annotated, Any, AsyncIterator, Callable, Optional, get_args, get_origin


@dataclass(frozen=True)
class DependMarker:
    """Internal marker for dependency injection."""

    dependency: Optional[Callable[..., Any]] = None
    use_cache: bool = True


def Depend(  # noqa: N802
    dependency: Optional[Callable[..., Any]] = None,
    *,
    use_cache: bool = True,
) -> Any:
    """Marker for dependency injection, mirroring FastAPI's Depends syntax.

    Returns Any to allow usage as a default parameter value:
        def my_func(service: MyService = Depend(get_service)):
            ...
    """
    return DependMarker(dependency=dependency, use_cache=use_cache)


def _depend_from_annotation(annotation: Any) -> DependMarker | None:
    origin = get_origin(annotation)
    if origin is not Annotated:
        return None
    metadata = get_args(annotation)[1:]
    for meta in metadata:
        if isinstance(meta, DependMarker):
            return meta
    return None


def _dependency_marker(parameter: inspect.Parameter) -> DependMarker | None:
    if isinstance(parameter.default, DependMarker):
        return parameter.default
    return _depend_from_annotation(parameter.annotation)


class _DependencyResolver:
    """Resolve dependency graphs for a callable, including context manager lifetimes."""

    def __init__(self, initial_kwargs: Optional[dict[str, Any]] = None) -> None:
        self._context: dict[str, Any] = dict(initial_kwargs or {})
        self._cache: dict[Callable[..., Any], Any] = {}
        self._active: set[Callable[..., Any]] = set()
        self._stack = AsyncExitStack()

    async def close(self) -> None:
        await self._stack.aclose()

    async def build_call_kwargs(self, func: Callable[..., Any]) -> dict[str, Any]:
        call_kwargs: dict[str, Any] = {}
        signature = inspect.signature(func)
        func_name = func.__name__ if hasattr(func, "__name__") else func.__class__.__name__
        for name, parameter in signature.parameters.items():
            if parameter.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            if name in self._context:
                call_kwargs[name] = self._context[name]
                continue
            marker = _dependency_marker(parameter)
            if marker is not None:
                value = await self._resolve_dependency(marker)
                self._context[name] = value
                call_kwargs[name] = value
                continue
            if parameter.default is not inspect.Parameter.empty:
                call_kwargs[name] = parameter.default
                self._context.setdefault(name, parameter.default)
                continue
            raise TypeError(f"Missing required parameter '{name}' for {func_name}")
        return call_kwargs

    async def _resolve_dependency(self, marker: DependMarker) -> Any:
        dependency = marker.dependency
        if dependency is None:
            raise TypeError("Depend requires a dependency callable")
        if marker.use_cache and dependency in self._cache:
            return self._cache[dependency]
        if dependency in self._active:
            name = (
                dependency.__name__
                if hasattr(dependency, "__name__")
                else dependency.__class__.__name__
            )
            raise RuntimeError(f"Circular dependency detected for {name}")
        self._active.add(dependency)
        try:
            kwargs = await self.build_call_kwargs(dependency)
            value = await self._call_dependency(dependency, kwargs)
            if marker.use_cache:
                self._cache[dependency] = value
            return value
        finally:
            self._active.discard(dependency)

    async def _call_dependency(
        self,
        dependency: Callable[..., Any],
        kwargs: dict[str, Any],
    ) -> Any:
        if inspect.isasyncgenfunction(dependency):
            context_manager = asynccontextmanager(dependency)(**kwargs)
            return await self._stack.enter_async_context(context_manager)
        if inspect.isgeneratorfunction(dependency):
            context_manager = contextmanager(dependency)(**kwargs)
            return self._stack.enter_context(context_manager)
        result = dependency(**kwargs)
        resolved = await self._await_if_needed(result)
        return await self._enter_context_if_needed(resolved)

    async def _await_if_needed(self, value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

    async def _enter_context_if_needed(self, value: Any) -> Any:
        if hasattr(value, "__aenter__") and hasattr(value, "__aexit__"):
            return await self._stack.enter_async_context(value)  # type: ignore[arg-type]
        if hasattr(value, "__enter__") and hasattr(value, "__exit__"):
            return self._stack.enter_context(value)  # type: ignore[arg-type]
        return value


@asynccontextmanager
async def provide_dependencies(
    func: Callable[..., Any],
    kwargs: Optional[dict[str, Any]] = None,
) -> AsyncIterator[dict[str, Any]]:
    """Resolve dependencies for ``func`` and manage their lifetimes."""

    resolver = _DependencyResolver(kwargs)
    try:
        call_kwargs = await resolver.build_call_kwargs(func)
        yield call_kwargs
    finally:
        await resolver.close()
