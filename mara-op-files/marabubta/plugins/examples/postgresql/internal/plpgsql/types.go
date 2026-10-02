// Marabunta - Licensed under the MIT License.
// Package plpgsql implements a PL/pgSQL engine for the distributed PostgreSQL
// plugin. It provides a lexer, parser, AST, type system, and function catalog
// for CREATE FUNCTION, CREATE PROCEDURE, CALL, and DO $$ blocks.
package plpgsql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// PLType represents a PL/pgSQL data type. Each concrete type implements this
// interface to provide zero-value construction, type naming, and display.
type PLType interface {
	// TypeName returns the canonical PostgreSQL type name (e.g. "integer", "text").
	TypeName() string
	// Zero returns the zero/default value for this type.
	Zero() PLValue
	// String returns a human-readable representation of the type.
	String() string
}

// PLValue holds a typed value in the PL/pgSQL runtime. IsNull indicates SQL NULL
// semantics — when true, Value should be ignored.
type PLValue struct {
	Type   PLType
	Value  interface{}
	IsNull bool
}

// NullValue creates a NULL PLValue of the given type.
func NullValue(t PLType) PLValue {
	return PLValue{Type: t, Value: nil, IsNull: true}
}

// NewPLValue creates a non-null PLValue with the given type and value.
func NewPLValue(t PLType, v interface{}) PLValue {
	return PLValue{Type: t, Value: v, IsNull: false}
}

// String returns a display representation of the value.
func (v PLValue) String() string {
	if v.IsNull {
		return "NULL"
	}
	if v.Value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v.Value)
}

// --------------------------------------------------------------------------
// Concrete types
// --------------------------------------------------------------------------

// PLInteger represents the PostgreSQL integer (int4) type, a 32-bit signed integer.
type PLInteger struct{}

func (PLInteger) TypeName() string { return "integer" }
func (PLInteger) String() string   { return "integer" }
func (PLInteger) Zero() PLValue {
	return PLValue{Type: PLInteger{}, Value: int64(0), IsNull: false}
}

// PLBigint represents the PostgreSQL bigint (int8) type, a 64-bit signed integer.
type PLBigint struct{}

func (PLBigint) TypeName() string { return "bigint" }
func (PLBigint) String() string   { return "bigint" }
func (PLBigint) Zero() PLValue {
	return PLValue{Type: PLBigint{}, Value: int64(0), IsNull: false}
}

// PLNumeric represents the PostgreSQL numeric/decimal type, stored as float64.
type PLNumeric struct{}

func (PLNumeric) TypeName() string { return "numeric" }
func (PLNumeric) String() string   { return "numeric" }
func (PLNumeric) Zero() PLValue {
	return PLValue{Type: PLNumeric{}, Value: float64(0), IsNull: false}
}

// PLText represents the PostgreSQL text/varchar/char types.
type PLText struct{}

func (PLText) TypeName() string { return "text" }
func (PLText) String() string   { return "text" }
func (PLText) Zero() PLValue {
	return PLValue{Type: PLText{}, Value: "", IsNull: false}
}

// PLBoolean represents the PostgreSQL boolean type.
type PLBoolean struct{}

func (PLBoolean) TypeName() string { return "boolean" }
func (PLBoolean) String() string   { return "boolean" }
func (PLBoolean) Zero() PLValue {
	return PLValue{Type: PLBoolean{}, Value: false, IsNull: false}
}

// PLDate represents the PostgreSQL date type.
type PLDate struct{}

func (PLDate) TypeName() string { return "date" }
func (PLDate) String() string   { return "date" }
func (PLDate) Zero() PLValue {
	return PLValue{Type: PLDate{}, Value: time.Time{}, IsNull: false}
}

// PLTimestamp represents the PostgreSQL timestamp type.
type PLTimestamp struct{}

func (PLTimestamp) TypeName() string { return "timestamp" }
func (PLTimestamp) String() string   { return "timestamp" }
func (PLTimestamp) Zero() PLValue {
	return PLValue{Type: PLTimestamp{}, Value: time.Time{}, IsNull: false}
}

// PLVoid represents the void return type (for procedures with no return value).
type PLVoid struct{}

func (PLVoid) TypeName() string { return "void" }
func (PLVoid) String() string   { return "void" }
func (PLVoid) Zero() PLValue {
	return PLValue{Type: PLVoid{}, Value: nil, IsNull: true}
}

// PLRecord represents a composite/row type as a map of column names to values.
type PLRecord struct{}

func (PLRecord) TypeName() string { return "record" }
func (PLRecord) String() string   { return "record" }
func (PLRecord) Zero() PLValue {
	return PLValue{Type: PLRecord{}, Value: map[string]PLValue{}, IsNull: false}
}

// --------------------------------------------------------------------------
// Type resolution
// --------------------------------------------------------------------------

// ResolveType maps a SQL type name string to the corresponding PLType.
// It handles common aliases: "int", "int4" -> PLInteger, "varchar" -> PLText, etc.
// The match is case-insensitive.
func ResolveType(typeName string) (PLType, error) {
	normalized := strings.ToLower(strings.TrimSpace(typeName))

	// Strip parenthesized precision/scale (e.g. "numeric(10,2)", "varchar(255)").
	if idx := strings.Index(normalized, "("); idx >= 0 {
		normalized = strings.TrimSpace(normalized[:idx])
	}

	switch normalized {
	case "integer", "int", "int4", "serial":
		return PLInteger{}, nil
	case "bigint", "int8", "bigserial":
		return PLBigint{}, nil
	case "numeric", "decimal", "real", "float", "float4", "float8", "double precision":
		return PLNumeric{}, nil
	case "text", "varchar", "character varying", "char", "character", "name":
		return PLText{}, nil
	case "boolean", "bool":
		return PLBoolean{}, nil
	case "date":
		return PLDate{}, nil
	case "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		return PLTimestamp{}, nil
	case "void":
		return PLVoid{}, nil
	case "record":
		return PLRecord{}, nil
	default:
		return nil, fmt.Errorf("unknown PL/pgSQL type: %q", typeName)
	}
}

// --------------------------------------------------------------------------
// Type coercion
// --------------------------------------------------------------------------

// Coerce converts a PLValue to the target PLType. It returns an error if the
// conversion is not possible. NULL values are preserved across coercions (the
// type changes but IsNull remains true).
func Coerce(val PLValue, target PLType) (PLValue, error) {
	// NULL propagation: changing type of NULL yields NULL of new type.
	if val.IsNull {
		return NullValue(target), nil
	}

	srcName := val.Type.TypeName()
	dstName := target.TypeName()

	// Identity coercion.
	if srcName == dstName {
		return val, nil
	}

	switch target.(type) {
	case PLText:
		return coerceToText(val)
	case PLInteger:
		return coerceToInteger(val)
	case PLBigint:
		return coerceToBigint(val)
	case PLNumeric:
		return coerceToNumeric(val)
	case PLBoolean:
		return coerceToBoolean(val)
	case PLVoid:
		return PLValue{Type: PLVoid{}, Value: nil, IsNull: true}, nil
	default:
		return PLValue{}, fmt.Errorf("cannot coerce %s to %s", srcName, dstName)
	}
}

// coerceToText converts any value to its text representation.
func coerceToText(val PLValue) (PLValue, error) {
	var s string
	switch v := val.Value.(type) {
	case int64:
		s = strconv.FormatInt(v, 10)
	case float64:
		if v == math.Trunc(v) && !math.IsInf(v, 0) {
			s = strconv.FormatInt(int64(v), 10)
		} else {
			s = strconv.FormatFloat(v, 'f', -1, 64)
		}
	case bool:
		if v {
			s = "true"
		} else {
			s = "false"
		}
	case string:
		s = v
	case time.Time:
		s = v.Format("2006-01-02 15:04:05")
	default:
		s = fmt.Sprintf("%v", v)
	}
	return NewPLValue(PLText{}, s), nil
}

// coerceToInteger converts text, bigint, numeric, or boolean to integer.
func coerceToInteger(val PLValue) (PLValue, error) {
	switch v := val.Value.(type) {
	case int64:
		if v > math.MaxInt32 || v < math.MinInt32 {
			return PLValue{}, fmt.Errorf("integer out of range: %d", v)
		}
		return NewPLValue(PLInteger{}, v), nil
	case float64:
		rounded := math.Round(v)
		if rounded > math.MaxInt32 || rounded < math.MinInt32 {
			return PLValue{}, fmt.Errorf("integer out of range: %f", v)
		}
		return NewPLValue(PLInteger{}, int64(rounded)), nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			// Try parsing as float first, then truncate.
			f, ferr := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if ferr != nil {
				return PLValue{}, fmt.Errorf("cannot coerce %q to integer: %w", v, err)
			}
			n = int64(math.Round(f))
		}
		if n > math.MaxInt32 || n < math.MinInt32 {
			return PLValue{}, fmt.Errorf("integer out of range: %d", n)
		}
		return NewPLValue(PLInteger{}, n), nil
	case bool:
		if v {
			return NewPLValue(PLInteger{}, int64(1)), nil
		}
		return NewPLValue(PLInteger{}, int64(0)), nil
	default:
		return PLValue{}, fmt.Errorf("cannot coerce %T to integer", v)
	}
}

// coerceToBigint converts text, integer, numeric, or boolean to bigint.
func coerceToBigint(val PLValue) (PLValue, error) {
	switch v := val.Value.(type) {
	case int64:
		return NewPLValue(PLBigint{}, v), nil
	case float64:
		return NewPLValue(PLBigint{}, int64(math.Round(v))), nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			f, ferr := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if ferr != nil {
				return PLValue{}, fmt.Errorf("cannot coerce %q to bigint: %w", v, err)
			}
			n = int64(math.Round(f))
		}
		return NewPLValue(PLBigint{}, n), nil
	case bool:
		if v {
			return NewPLValue(PLBigint{}, int64(1)), nil
		}
		return NewPLValue(PLBigint{}, int64(0)), nil
	default:
		return PLValue{}, fmt.Errorf("cannot coerce %T to bigint", v)
	}
}

// coerceToNumeric converts text, integer, bigint, or boolean to numeric.
func coerceToNumeric(val PLValue) (PLValue, error) {
	switch v := val.Value.(type) {
	case int64:
		return NewPLValue(PLNumeric{}, float64(v)), nil
	case float64:
		return NewPLValue(PLNumeric{}, v), nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return PLValue{}, fmt.Errorf("cannot coerce %q to numeric: %w", v, err)
		}
		return NewPLValue(PLNumeric{}, f), nil
	case bool:
		if v {
			return NewPLValue(PLNumeric{}, float64(1)), nil
		}
		return NewPLValue(PLNumeric{}, float64(0)), nil
	default:
		return PLValue{}, fmt.Errorf("cannot coerce %T to numeric", v)
	}
}

// coerceToBoolean converts text, integer, or numeric to boolean.
func coerceToBoolean(val PLValue) (PLValue, error) {
	switch v := val.Value.(type) {
	case bool:
		return NewPLValue(PLBoolean{}, v), nil
	case int64:
		return NewPLValue(PLBoolean{}, v != 0), nil
	case float64:
		return NewPLValue(PLBoolean{}, v != 0), nil
	case string:
		lower := strings.ToLower(strings.TrimSpace(v))
		switch lower {
		case "true", "t", "yes", "y", "on", "1":
			return NewPLValue(PLBoolean{}, true), nil
		case "false", "f", "no", "n", "off", "0":
			return NewPLValue(PLBoolean{}, false), nil
		default:
			return PLValue{}, fmt.Errorf("cannot coerce %q to boolean", v)
		}
	default:
		return PLValue{}, fmt.Errorf("cannot coerce %T to boolean", v)
	}
}

// --------------------------------------------------------------------------
// NULL propagation helpers
// --------------------------------------------------------------------------

// IsNull returns true if the value is SQL NULL.
func IsNull(v PLValue) bool {
	return v.IsNull
}

// PropagateNull returns a NULL of the given type if any of the input values
// are NULL. This implements standard SQL NULL propagation for expressions.
func PropagateNull(resultType PLType, values ...PLValue) (PLValue, bool) {
	for _, v := range values {
		if v.IsNull {
			return NullValue(resultType), true
		}
	}
	return PLValue{}, false
}
