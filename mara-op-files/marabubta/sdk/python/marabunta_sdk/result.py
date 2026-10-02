# Marabunta - Licensed under the MIT License.
"""
Type-Safe Result Deserialization for the Marabunta SDK.

Provides utilities for safely deserializing job results
with proper error handling and type conversion.

Example:
    from marabunta_sdk import TypedResult

    # Parse a typed result
    raw = {"value": 3.14, "iterations": 100}
    result = TypedResult.from_dict(raw, ComputeResult)

    print(f"Value: {result.value.value}")
    print(f"Raw: {result.raw}")
"""

from dataclasses import dataclass
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

T = TypeVar("T")


class ResultError(Exception):
    """Error during result handling."""

    pass


class DeserializationError(ResultError):
    """Failed to deserialize result."""

    def __init__(self, message: str):
        super().__init__(f"Failed to deserialize result: {message}")


class EmptyResultError(ResultError):
    """Result is empty."""

    def __init__(self):
        super().__init__("Result is empty")


class TypeMismatchError(ResultError):
    """Result type mismatch."""

    def __init__(self, expected: str, actual: str):
        self.expected = expected
        self.actual = actual
        super().__init__(f"Result type mismatch: expected {expected}, got {actual}")


class MissingFieldError(ResultError):
    """Missing required field."""

    def __init__(self, field: str):
        self.field = field
        super().__init__(f"Missing field: {field}")


class InvalidFieldError(ResultError):
    """Invalid field value."""

    def __init__(self, field: str, message: str):
        self.field = field
        super().__init__(f"Invalid field value for '{field}': {message}")


@dataclass
class TypedResult(Generic[T]):
    """
    A type-safe wrapper around job results.

    Example:
        result = TypedResult.from_dict(data, MyResultClass)
        print(result.value)
        print(result.raw)
    """

    value: T
    raw: Any

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        result_type: Type[T],
    ) -> "TypedResult[T]":
        """
        Create a typed result from a dictionary.

        Args:
            data: Raw dictionary data
            result_type: Expected result type class

        Returns:
            TypedResult instance

        Raises:
            DeserializationError: If deserialization fails
        """
        try:
            # Try to instantiate the type
            if hasattr(result_type, "from_dict"):
                value = result_type.from_dict(data)  # type: ignore
            elif hasattr(result_type, "__dataclass_fields__"):
                # Dataclass
                value = result_type(**data)
            else:
                # Assume it's a simple type or dict
                value = data  # type: ignore
            return cls(value=value, raw=data)
        except Exception as e:
            raise DeserializationError(str(e))

    @classmethod
    def from_json(cls, json_str: str, result_type: Type[T]) -> "TypedResult[T]":
        """
        Create a typed result from a JSON string.

        Args:
            json_str: JSON string
            result_type: Expected result type class

        Returns:
            TypedResult instance
        """
        import json

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise DeserializationError(f"Invalid JSON: {e}")

        return cls.from_dict(data, result_type)

    def into_value(self) -> T:
        """Consume and return the inner value."""
        return self.value

    def get_field(self, field: str, field_type: Type = None) -> Any:
        """
        Get a field from the raw data.

        Args:
            field: Field name
            field_type: Expected type (optional, for documentation)

        Returns:
            Field value

        Raises:
            MissingFieldError: If field doesn't exist
        """
        if not isinstance(self.raw, dict):
            raise MissingFieldError(field)

        if field not in self.raw:
            raise MissingFieldError(field)

        return self.raw[field]

    def has_field(self, field: str) -> bool:
        """Check if a field exists in the raw data."""
        if not isinstance(self.raw, dict):
            return False
        return field in self.raw


class MultiTypeResult:
    """
    A result that can be one of several types.

    Useful when the result type is not known at compile time.
    """

    def __init__(self, value: Any):
        self._value = value

    @classmethod
    def from_json(cls, data: Any) -> "MultiTypeResult":
        """Create from any value."""
        return cls(data)

    @property
    def value(self) -> Any:
        """Get the raw value."""
        return self._value

    def as_string(self) -> Optional[str]:
        """Try to get as a string."""
        if isinstance(self._value, str):
            return self._value
        return None

    def as_int(self) -> Optional[int]:
        """Try to get as an integer."""
        if isinstance(self._value, int) and not isinstance(self._value, bool):
            return self._value
        if isinstance(self._value, float):
            return int(self._value)
        return None

    def as_float(self) -> Optional[float]:
        """Try to get as a float."""
        if isinstance(self._value, (int, float)) and not isinstance(self._value, bool):
            return float(self._value)
        return None

    def as_bool(self) -> Optional[bool]:
        """Try to get as a boolean."""
        if isinstance(self._value, bool):
            return self._value
        return None

    def as_list(self) -> Optional[List[Any]]:
        """Try to get as a list."""
        if isinstance(self._value, list):
            return self._value
        return None

    def as_dict(self) -> Optional[Dict[str, Any]]:
        """Try to get as a dictionary."""
        if isinstance(self._value, dict):
            return self._value
        return None

    def is_null(self) -> bool:
        """Check if the value is null/None."""
        return self._value is None

    @property
    def type_name(self) -> str:
        """Get the type name."""
        if self._value is None:
            return "null"
        if isinstance(self._value, bool):
            return "boolean"
        if isinstance(self._value, int):
            return "integer"
        if isinstance(self._value, float):
            return "float"
        if isinstance(self._value, str):
            return "string"
        if isinstance(self._value, list):
            return "array"
        if isinstance(self._value, dict):
            return "object"
        return type(self._value).__name__


@dataclass
class ValidatedResult(Generic[T]):
    """
    A validated result with optional validation errors.

    Useful for results that might have warnings or non-fatal issues.
    """

    value: T
    validation_errors: List[str]

    @classmethod
    def valid(cls, value: T) -> "ValidatedResult[T]":
        """Create a valid result with no errors."""
        return cls(value=value, validation_errors=[])

    @classmethod
    def with_errors(cls, value: T, errors: List[str]) -> "ValidatedResult[T]":
        """Create a result with validation errors."""
        return cls(value=value, validation_errors=errors)

    @property
    def is_valid(self) -> bool:
        """Check if the result is valid (no errors)."""
        return len(self.validation_errors) == 0

    @property
    def errors(self) -> List[str]:
        """Get validation errors."""
        return self.validation_errors

    def valid_value(self) -> Optional[T]:
        """Get value only if valid."""
        if self.is_valid:
            return self.value
        return None


class ResultExtractor:
    """
    Builder for extracting and transforming results.

    Example:
        extractor = ResultExtractor(raw_data)
        value = extractor.field("result").as_int()
    """

    def __init__(self, data: Any):
        self._data = data

    def extract(self, result_type: Type[T]) -> T:
        """
        Extract the full result as a typed value.

        Args:
            result_type: Expected result type

        Returns:
            Typed value
        """
        result = TypedResult.from_dict(self._data, result_type)
        return result.value

    def field(self, name: str) -> "ResultExtractor":
        """
        Get a field from the data.

        Args:
            name: Field name

        Returns:
            New extractor for the field value
        """
        if not isinstance(self._data, dict):
            raise MissingFieldError(name)
        if name not in self._data:
            raise MissingFieldError(name)
        return ResultExtractor(self._data[name])

    def as_string(self) -> str:
        """Get as string."""
        if not isinstance(self._data, str):
            raise TypeMismatchError("string", type(self._data).__name__)
        return self._data

    def as_int(self) -> int:
        """Get as integer."""
        if isinstance(self._data, bool):
            raise TypeMismatchError("int", "bool")
        if not isinstance(self._data, (int, float)):
            raise TypeMismatchError("int", type(self._data).__name__)
        return int(self._data)

    def as_float(self) -> float:
        """Get as float."""
        if isinstance(self._data, bool):
            raise TypeMismatchError("float", "bool")
        if not isinstance(self._data, (int, float)):
            raise TypeMismatchError("float", type(self._data).__name__)
        return float(self._data)

    def as_bool(self) -> bool:
        """Get as boolean."""
        if not isinstance(self._data, bool):
            raise TypeMismatchError("bool", type(self._data).__name__)
        return self._data

    def as_list(self) -> List[Any]:
        """Get as list."""
        if not isinstance(self._data, list):
            raise TypeMismatchError("list", type(self._data).__name__)
        return self._data

    def as_dict(self) -> Dict[str, Any]:
        """Get as dictionary."""
        if not isinstance(self._data, dict):
            raise TypeMismatchError("dict", type(self._data).__name__)
        return self._data

    def map(self, func) -> Any:
        """
        Map the data with a transformation function.

        Args:
            func: Transformation function

        Returns:
            Transformed value
        """
        return func(self._data)

    def or_default(self, default: Any) -> Any:
        """
        Return data or default if None.

        Args:
            default: Default value

        Returns:
            Data or default
        """
        if self._data is None:
            return default
        return self._data
