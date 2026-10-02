# Marabunta - Licensed under the MIT License.
"""
Tests for type-safe result deserialization.
"""

import pytest
from dataclasses import dataclass
from typing import List
from marabunta_sdk import (
    TypedResult,
    MultiTypeResult,
    ValidatedResult,
    ResultExtractor,
    ResultError,
    DeserializationError,
    MissingFieldError,
    TypeMismatchError,
)


@dataclass
class SampleResult:
    """Sample result type for testing."""
    value: int
    name: str


class TestTypedResult:
    """Test TypedResult."""

    def test_from_dict(self):
        """Test creating from dict."""
        data = {"value": 42, "name": "test"}
        result = TypedResult.from_dict(data, SampleResult)

        assert result.value.value == 42
        assert result.value.name == "test"
        assert result.raw == data

    def test_from_json(self):
        """Test creating from JSON string."""
        json_str = '{"value": 42, "name": "test"}'
        result = TypedResult.from_json(json_str, SampleResult)

        assert result.value.value == 42
        assert result.value.name == "test"

    def test_from_json_invalid(self):
        """Test invalid JSON."""
        with pytest.raises(DeserializationError):
            TypedResult.from_json("not valid json", SampleResult)

    def test_into_value(self):
        """Test into_value."""
        data = {"value": 42, "name": "test"}
        result = TypedResult.from_dict(data, SampleResult)
        value = result.into_value()

        assert value.value == 42

    def test_get_field(self):
        """Test getting a field from raw data."""
        data = {"value": 42, "name": "test", "extra": "field"}
        result = TypedResult.from_dict(data, dict)

        assert result.get_field("extra") == "field"

    def test_get_field_missing(self):
        """Test getting missing field."""
        data = {"value": 42}
        result = TypedResult.from_dict(data, dict)

        with pytest.raises(MissingFieldError):
            result.get_field("missing")

    def test_has_field(self):
        """Test has_field."""
        data = {"value": 42}
        result = TypedResult.from_dict(data, dict)

        assert result.has_field("value")
        assert not result.has_field("missing")


class TestMultiTypeResult:
    """Test MultiTypeResult."""

    def test_string_value(self):
        """Test string value."""
        result = MultiTypeResult.from_json("hello")
        assert result.as_string() == "hello"
        assert result.type_name == "string"

    def test_int_value(self):
        """Test integer value."""
        result = MultiTypeResult.from_json(42)
        assert result.as_int() == 42
        assert result.type_name == "integer"

    def test_float_value(self):
        """Test float value."""
        result = MultiTypeResult.from_json(3.14)
        assert result.as_float() == 3.14
        assert result.type_name == "float"

    def test_bool_value(self):
        """Test boolean value."""
        result = MultiTypeResult.from_json(True)
        assert result.as_bool() is True
        assert result.type_name == "boolean"

    def test_list_value(self):
        """Test list value."""
        result = MultiTypeResult.from_json([1, 2, 3])
        assert result.as_list() == [1, 2, 3]
        assert result.type_name == "array"

    def test_dict_value(self):
        """Test dict value."""
        result = MultiTypeResult.from_json({"key": "value"})
        assert result.as_dict() == {"key": "value"}
        assert result.type_name == "object"

    def test_null_value(self):
        """Test null value."""
        result = MultiTypeResult.from_json(None)
        assert result.is_null()
        assert result.type_name == "null"

    def test_type_coercion(self):
        """Test type coercion."""
        result = MultiTypeResult.from_json(42)
        assert result.as_float() == 42.0

        result = MultiTypeResult.from_json(3.14)
        assert result.as_int() == 3

    def test_invalid_type_returns_none(self):
        """Test invalid type returns None."""
        result = MultiTypeResult.from_json("hello")
        assert result.as_int() is None
        assert result.as_float() is None
        assert result.as_bool() is None
        assert result.as_list() is None
        assert result.as_dict() is None


class TestValidatedResult:
    """Test ValidatedResult."""

    def test_valid_result(self):
        """Test valid result."""
        result = ValidatedResult.valid(42)
        assert result.is_valid
        assert result.value == 42
        assert result.errors == []
        assert result.valid_value() == 42

    def test_result_with_errors(self):
        """Test result with errors."""
        result = ValidatedResult.with_errors(42, ["warning1", "warning2"])
        assert not result.is_valid
        assert result.value == 42
        assert len(result.errors) == 2
        assert result.valid_value() is None


class TestResultExtractor:
    """Test ResultExtractor."""

    def test_extract(self):
        """Test extracting typed value."""
        data = {"value": 42, "name": "test"}
        extractor = ResultExtractor(data)
        result = extractor.extract(dict)

        assert result == data

    def test_field(self):
        """Test extracting a field."""
        data = {"outer": {"inner": "value"}}
        extractor = ResultExtractor(data)
        inner = extractor.field("outer").field("inner").as_string()

        assert inner == "value"

    def test_field_missing(self):
        """Test missing field."""
        data = {"key": "value"}
        extractor = ResultExtractor(data)

        with pytest.raises(MissingFieldError):
            extractor.field("missing")

    def test_as_string(self):
        """Test as_string."""
        extractor = ResultExtractor("hello")
        assert extractor.as_string() == "hello"

    def test_as_string_type_error(self):
        """Test as_string with wrong type."""
        extractor = ResultExtractor(42)
        with pytest.raises(TypeMismatchError):
            extractor.as_string()

    def test_as_int(self):
        """Test as_int."""
        extractor = ResultExtractor(42)
        assert extractor.as_int() == 42

    def test_as_int_from_float(self):
        """Test as_int from float."""
        extractor = ResultExtractor(3.14)
        assert extractor.as_int() == 3

    def test_as_int_type_error(self):
        """Test as_int with wrong type."""
        extractor = ResultExtractor("not a number")
        with pytest.raises(TypeMismatchError):
            extractor.as_int()

    def test_as_int_rejects_bool(self):
        """Test as_int rejects bool."""
        extractor = ResultExtractor(True)
        with pytest.raises(TypeMismatchError):
            extractor.as_int()

    def test_as_float(self):
        """Test as_float."""
        extractor = ResultExtractor(3.14)
        assert extractor.as_float() == 3.14

    def test_as_bool(self):
        """Test as_bool."""
        extractor = ResultExtractor(True)
        assert extractor.as_bool() is True

    def test_as_list(self):
        """Test as_list."""
        extractor = ResultExtractor([1, 2, 3])
        assert extractor.as_list() == [1, 2, 3]

    def test_as_dict(self):
        """Test as_dict."""
        extractor = ResultExtractor({"key": "value"})
        assert extractor.as_dict() == {"key": "value"}

    def test_map(self):
        """Test map transformation."""
        extractor = ResultExtractor({"value": 10})
        result = extractor.map(lambda d: d["value"] * 2)
        assert result == 20

    def test_or_default(self):
        """Test or_default."""
        extractor = ResultExtractor(None)
        assert extractor.or_default("default") == "default"

        extractor = ResultExtractor("actual")
        assert extractor.or_default("default") == "actual"

    def test_chained_extraction(self):
        """Test chained field extraction."""
        data = {
            "user": {
                "profile": {
                    "name": "Alice",
                    "age": 30
                }
            }
        }
        extractor = ResultExtractor(data)
        name = extractor.field("user").field("profile").field("name").as_string()
        age = extractor.field("user").field("profile").field("age").as_int()

        assert name == "Alice"
        assert age == 30
