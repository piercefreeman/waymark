from typing import Any, Callable

from mountaineer_di import (
    DependencyResolver,
    Depends,
    get_function_dependencies,
    isolate_dependency_only_function,
    provide_dependencies,
    strip_depends_from_signature,
)

DependMarker = type(Depends())
DependsMarker = DependMarker


def Depend(  # noqa: N802
    dependency: Callable[..., Any] | None = None,
    *,
    use_cache: bool = True,
) -> Any:
    """Compatibility alias for ``mountaineer_di.Depends``."""

    return Depends(dependency, use_cache=use_cache)


__all__ = [
    "DependencyResolver",
    "Depend",
    "DependMarker",
    "Depends",
    "DependsMarker",
    "get_function_dependencies",
    "isolate_dependency_only_function",
    "provide_dependencies",
    "strip_depends_from_signature",
]
