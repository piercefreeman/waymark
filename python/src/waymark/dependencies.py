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
Depend = Depends


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
