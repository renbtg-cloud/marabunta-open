// Marabunta - Licensed under the MIT License.
package plpgsql

import (
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// BuiltinFunc is the signature for a built-in PL/pgSQL function.
type BuiltinFunc func(args []PLValue) (PLValue, error)

// BuiltinRegistry maps lowercase function names to their implementations.
type BuiltinRegistry struct {
	funcs map[string]BuiltinFunc
}

// NewBuiltinRegistry creates a BuiltinRegistry populated with all standard
// built-in functions.
func NewBuiltinRegistry() *BuiltinRegistry {
	r := &BuiltinRegistry{funcs: make(map[string]BuiltinFunc)}
	r.registerStringFunctions()
	r.registerNumericFunctions()
	r.registerDateFunctions()
	r.registerNullFunctions()
	r.registerTypeFunctions()
	r.registerArrayFunctions()
	return r
}

// Has returns true if the function name is registered.
func (r *BuiltinRegistry) Has(name string) bool {
	_, ok := r.funcs[strings.ToLower(name)]
	return ok
}

// Call invokes a built-in function by name.
func (r *BuiltinRegistry) Call(name string, args []PLValue) (PLValue, error) {
	fn, ok := r.funcs[strings.ToLower(name)]
	if !ok {
		return PLValue{}, fmt.Errorf("unknown built-in function: %s", name)
	}
	return fn(args)
}

// ---------------------------------------------------------------------------
// String functions
// ---------------------------------------------------------------------------

func (r *BuiltinRegistry) registerStringFunctions() {
	r.funcs["length"] = builtinLength
	r.funcs["char_length"] = builtinLength
	r.funcs["character_length"] = builtinLength
	r.funcs["upper"] = builtinUpper
	r.funcs["lower"] = builtinLower
	r.funcs["trim"] = builtinTrim
	r.funcs["ltrim"] = builtinLTrim
	r.funcs["rtrim"] = builtinRTrim
	r.funcs["substring"] = builtinSubstring
	r.funcs["substr"] = builtinSubstring
	r.funcs["replace"] = builtinReplace
	r.funcs["position"] = builtinPosition
	r.funcs["strpos"] = builtinPosition
	r.funcs["concat"] = builtinConcat
	r.funcs["concat_ws"] = builtinConcatWS
	r.funcs["format"] = builtinFormat
	r.funcs["left"] = builtinLeft
	r.funcs["right"] = builtinRight
	r.funcs["repeat"] = builtinRepeat
	r.funcs["reverse"] = builtinReverse
	r.funcs["split_part"] = builtinSplitPart
	r.funcs["regexp_matches"] = builtinRegexpMatches
}

func builtinLength(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("length requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLInteger{}), nil
	}
	s := plValueToString(args[0])
	return NewPLValue(PLInteger{}, int64(len([]rune(s)))), nil
}

func builtinUpper(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("upper requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	return NewPLValue(PLText{}, strings.ToUpper(plValueToString(args[0]))), nil
}

func builtinLower(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("lower requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	return NewPLValue(PLText{}, strings.ToLower(plValueToString(args[0]))), nil
}

func builtinTrim(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("trim requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	return NewPLValue(PLText{}, strings.TrimSpace(plValueToString(args[0]))), nil
}

func builtinLTrim(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("ltrim requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	s := plValueToString(args[0])
	if len(args) >= 2 && !args[1].IsNull {
		chars := plValueToString(args[1])
		return NewPLValue(PLText{}, strings.TrimLeft(s, chars)), nil
	}
	return NewPLValue(PLText{}, strings.TrimLeft(s, " \t\n\r")), nil
}

func builtinRTrim(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("rtrim requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	s := plValueToString(args[0])
	if len(args) >= 2 && !args[1].IsNull {
		chars := plValueToString(args[1])
		return NewPLValue(PLText{}, strings.TrimRight(s, chars)), nil
	}
	return NewPLValue(PLText{}, strings.TrimRight(s, " \t\n\r")), nil
}

func builtinSubstring(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("substring requires at least 2 arguments")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	runes := []rune(plValueToString(args[0]))
	start, err := plValueToInt(args[1])
	if err != nil {
		return PLValue{}, fmt.Errorf("substring start: %w", err)
	}
	// PL/pgSQL uses 1-based indexing.
	startIdx := int(start) - 1
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(runes) {
		return NewPLValue(PLText{}, ""), nil
	}

	if len(args) >= 3 && !args[2].IsNull {
		length, err := plValueToInt(args[2])
		if err != nil {
			return PLValue{}, fmt.Errorf("substring length: %w", err)
		}
		endIdx := startIdx + int(length)
		if endIdx > len(runes) {
			endIdx = len(runes)
		}
		return NewPLValue(PLText{}, string(runes[startIdx:endIdx])), nil
	}
	return NewPLValue(PLText{}, string(runes[startIdx:])), nil
}

func builtinReplace(args []PLValue) (PLValue, error) {
	if len(args) < 3 {
		return PLValue{}, fmt.Errorf("replace requires 3 arguments")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	s := plValueToString(args[0])
	from := plValueToString(args[1])
	to := plValueToString(args[2])
	return NewPLValue(PLText{}, strings.ReplaceAll(s, from, to)), nil
}

func builtinPosition(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("position requires 2 arguments")
	}
	if args[0].IsNull || args[1].IsNull {
		return NullValue(PLInteger{}), nil
	}
	substr := plValueToString(args[0])
	str := plValueToString(args[1])
	idx := strings.Index(str, substr)
	if idx < 0 {
		return NewPLValue(PLInteger{}, int64(0)), nil
	}
	// Return 1-based position.
	return NewPLValue(PLInteger{}, int64(idx+1)), nil
}

func builtinConcat(args []PLValue) (PLValue, error) {
	var sb strings.Builder
	for _, arg := range args {
		if !arg.IsNull {
			sb.WriteString(plValueToString(arg))
		}
	}
	return NewPLValue(PLText{}, sb.String()), nil
}

func builtinConcatWS(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("concat_ws requires at least 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	sep := plValueToString(args[0])
	var parts []string
	for _, arg := range args[1:] {
		if !arg.IsNull {
			parts = append(parts, plValueToString(arg))
		}
	}
	return NewPLValue(PLText{}, strings.Join(parts, sep)), nil
}

func builtinFormat(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("format requires at least 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	fmtStr := plValueToString(args[0])
	argIdx := 1
	var sb strings.Builder
	for i := 0; i < len(fmtStr); i++ {
		if fmtStr[i] == '%' && i+1 < len(fmtStr) {
			next := fmtStr[i+1]
			switch next {
			case 's': // value as string
				if argIdx < len(args) {
					if args[argIdx].IsNull {
						// NULL in format %s — omit.
					} else {
						sb.WriteString(plValueToString(args[argIdx]))
					}
					argIdx++
				}
				i++
			case 'I': // identifier (quoted)
				if argIdx < len(args) {
					sb.WriteByte('"')
					sb.WriteString(plValueToString(args[argIdx]))
					sb.WriteByte('"')
					argIdx++
				}
				i++
			case 'L': // literal (quoted)
				if argIdx < len(args) {
					if args[argIdx].IsNull {
						sb.WriteString("NULL")
					} else {
						sb.WriteByte('\'')
						sb.WriteString(strings.ReplaceAll(plValueToString(args[argIdx]), "'", "''"))
						sb.WriteByte('\'')
					}
					argIdx++
				}
				i++
			case '%':
				sb.WriteByte('%')
				i++
			default:
				sb.WriteByte('%')
			}
		} else {
			sb.WriteByte(fmtStr[i])
		}
	}
	return NewPLValue(PLText{}, sb.String()), nil
}

func builtinLeft(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("left requires 2 arguments")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	runes := []rune(plValueToString(args[0]))
	n, err := plValueToInt(args[1])
	if err != nil {
		return PLValue{}, err
	}
	if n < 0 {
		// left(s, -n) = all but last n characters.
		end := len(runes) + int(n)
		if end < 0 {
			end = 0
		}
		return NewPLValue(PLText{}, string(runes[:end])), nil
	}
	if int(n) > len(runes) {
		n = int64(len(runes))
	}
	return NewPLValue(PLText{}, string(runes[:n])), nil
}

func builtinRight(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("right requires 2 arguments")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	runes := []rune(plValueToString(args[0]))
	n, err := plValueToInt(args[1])
	if err != nil {
		return PLValue{}, err
	}
	if n < 0 {
		// right(s, -n) = all but first n characters.
		start := int(-n)
		if start > len(runes) {
			start = len(runes)
		}
		return NewPLValue(PLText{}, string(runes[start:])), nil
	}
	start := len(runes) - int(n)
	if start < 0 {
		start = 0
	}
	return NewPLValue(PLText{}, string(runes[start:])), nil
}

func builtinRepeat(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("repeat requires 2 arguments")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	s := plValueToString(args[0])
	n, err := plValueToInt(args[1])
	if err != nil {
		return PLValue{}, err
	}
	if n <= 0 {
		return NewPLValue(PLText{}, ""), nil
	}
	return NewPLValue(PLText{}, strings.Repeat(s, int(n))), nil
}

func builtinReverse(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("reverse requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	runes := []rune(plValueToString(args[0]))
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return NewPLValue(PLText{}, string(runes)), nil
}

func builtinSplitPart(args []PLValue) (PLValue, error) {
	if len(args) < 3 {
		return PLValue{}, fmt.Errorf("split_part requires 3 arguments")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	s := plValueToString(args[0])
	delim := plValueToString(args[1])
	field, err := plValueToInt(args[2])
	if err != nil {
		return PLValue{}, err
	}
	parts := strings.Split(s, delim)
	idx := int(field) - 1 // 1-based
	if idx < 0 || idx >= len(parts) {
		return NewPLValue(PLText{}, ""), nil
	}
	return NewPLValue(PLText{}, parts[idx]), nil
}

func builtinRegexpMatches(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("regexp_matches requires 2 arguments")
	}
	if args[0].IsNull || args[1].IsNull {
		return NullValue(PLText{}), nil
	}
	text := plValueToString(args[0])
	pattern := plValueToString(args[1])
	re, err := regexp.Compile(pattern)
	if err != nil {
		return PLValue{}, fmt.Errorf("regexp_matches: invalid pattern: %w", err)
	}
	matches := re.FindStringSubmatch(text)
	if matches == nil {
		return NullValue(PLText{}), nil
	}
	// Return as a comma-separated string representing an array.
	if len(matches) > 1 {
		return NewPLValue(PLText{}, "{"+strings.Join(matches[1:], ",")+"}"), nil
	}
	return NewPLValue(PLText{}, "{"+matches[0]+"}"), nil
}

// ---------------------------------------------------------------------------
// Numeric functions
// ---------------------------------------------------------------------------

func (r *BuiltinRegistry) registerNumericFunctions() {
	r.funcs["abs"] = builtinAbs
	r.funcs["ceil"] = builtinCeil
	r.funcs["ceiling"] = builtinCeil
	r.funcs["floor"] = builtinFloor
	r.funcs["round"] = builtinRound
	r.funcs["trunc"] = builtinTrunc
	r.funcs["truncate"] = builtinTrunc
	r.funcs["mod"] = builtinMod
	r.funcs["power"] = builtinPower
	r.funcs["pow"] = builtinPower
	r.funcs["sqrt"] = builtinSqrt
	r.funcs["random"] = builtinRandom
	r.funcs["greatest"] = builtinGreatest
	r.funcs["least"] = builtinLeast
}

func builtinAbs(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("abs requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	f, ok := toFloat(args[0])
	if !ok {
		return PLValue{}, fmt.Errorf("abs: argument is not numeric")
	}
	result := math.Abs(f)
	if _, isInt := args[0].Value.(int64); isInt {
		return NewPLValue(PLInteger{}, int64(result)), nil
	}
	return NewPLValue(PLNumeric{}, result), nil
}

func builtinCeil(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("ceil requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	f, ok := toFloat(args[0])
	if !ok {
		return PLValue{}, fmt.Errorf("ceil: argument is not numeric")
	}
	return NewPLValue(PLNumeric{}, math.Ceil(f)), nil
}

func builtinFloor(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("floor requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	f, ok := toFloat(args[0])
	if !ok {
		return PLValue{}, fmt.Errorf("floor: argument is not numeric")
	}
	return NewPLValue(PLNumeric{}, math.Floor(f)), nil
}

func builtinRound(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("round requires at least 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	f, ok := toFloat(args[0])
	if !ok {
		return PLValue{}, fmt.Errorf("round: argument is not numeric")
	}
	places := 0
	if len(args) >= 2 && !args[1].IsNull {
		p, err := plValueToInt(args[1])
		if err != nil {
			return PLValue{}, err
		}
		places = int(p)
	}
	multiplier := math.Pow(10, float64(places))
	rounded := math.Round(f*multiplier) / multiplier
	if places == 0 {
		return NewPLValue(PLNumeric{}, rounded), nil
	}
	return NewPLValue(PLNumeric{}, rounded), nil
}

func builtinTrunc(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("trunc requires at least 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	f, ok := toFloat(args[0])
	if !ok {
		return PLValue{}, fmt.Errorf("trunc: argument is not numeric")
	}
	places := 0
	if len(args) >= 2 && !args[1].IsNull {
		p, err := plValueToInt(args[1])
		if err != nil {
			return PLValue{}, err
		}
		places = int(p)
	}
	multiplier := math.Pow(10, float64(places))
	truncated := math.Trunc(f*multiplier) / multiplier
	return NewPLValue(PLNumeric{}, truncated), nil
}

func builtinMod(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("mod requires 2 arguments")
	}
	if args[0].IsNull || args[1].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	a, ok1 := toFloat(args[0])
	b, ok2 := toFloat(args[1])
	if !ok1 || !ok2 {
		return PLValue{}, fmt.Errorf("mod: arguments are not numeric")
	}
	if b == 0 {
		return PLValue{}, &RaiseError{Level: "EXCEPTION", Message: "division by zero", SQLState: "22012"}
	}
	return NewPLValue(PLNumeric{}, math.Mod(a, b)), nil
}

func builtinPower(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("power requires 2 arguments")
	}
	if args[0].IsNull || args[1].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	base, ok1 := toFloat(args[0])
	exp, ok2 := toFloat(args[1])
	if !ok1 || !ok2 {
		return PLValue{}, fmt.Errorf("power: arguments are not numeric")
	}
	return NewPLValue(PLNumeric{}, math.Pow(base, exp)), nil
}

func builtinSqrt(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("sqrt requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	f, ok := toFloat(args[0])
	if !ok {
		return PLValue{}, fmt.Errorf("sqrt: argument is not numeric")
	}
	if f < 0 {
		return PLValue{}, fmt.Errorf("cannot take square root of negative number")
	}
	return NewPLValue(PLNumeric{}, math.Sqrt(f)), nil
}

func builtinRandom(args []PLValue) (PLValue, error) {
	return NewPLValue(PLNumeric{}, rand.Float64()), nil
}

func builtinGreatest(args []PLValue) (PLValue, error) {
	if len(args) == 0 {
		return NullValue(PLText{}), nil
	}
	best := args[0]
	for _, arg := range args[1:] {
		if arg.IsNull {
			continue
		}
		if best.IsNull {
			best = arg
			continue
		}
		lf, lok := toFloat(best)
		rf, rok := toFloat(arg)
		if lok && rok {
			if rf > lf {
				best = arg
			}
		} else {
			if plValueToString(arg) > plValueToString(best) {
				best = arg
			}
		}
	}
	return best, nil
}

func builtinLeast(args []PLValue) (PLValue, error) {
	if len(args) == 0 {
		return NullValue(PLText{}), nil
	}
	best := args[0]
	for _, arg := range args[1:] {
		if arg.IsNull {
			continue
		}
		if best.IsNull {
			best = arg
			continue
		}
		lf, lok := toFloat(best)
		rf, rok := toFloat(arg)
		if lok && rok {
			if rf < lf {
				best = arg
			}
		} else {
			if plValueToString(arg) < plValueToString(best) {
				best = arg
			}
		}
	}
	return best, nil
}

// ---------------------------------------------------------------------------
// Date/time functions
// ---------------------------------------------------------------------------

func (r *BuiltinRegistry) registerDateFunctions() {
	r.funcs["now"] = builtinNow
	r.funcs["current_timestamp"] = builtinNow
	r.funcs["current_date"] = builtinCurrentDate
	r.funcs["clock_timestamp"] = builtinClockTimestamp
	r.funcs["age"] = builtinAge
	r.funcs["extract"] = builtinExtract
	r.funcs["date_trunc"] = builtinDateTrunc
	r.funcs["date_part"] = builtinExtract
}

func builtinNow(args []PLValue) (PLValue, error) {
	return NewPLValue(PLTimestamp{}, time.Now()), nil
}

func builtinCurrentDate(args []PLValue) (PLValue, error) {
	now := time.Now()
	d := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	return NewPLValue(PLDate{}, d), nil
}

func builtinClockTimestamp(args []PLValue) (PLValue, error) {
	return NewPLValue(PLTimestamp{}, time.Now()), nil
}

func builtinAge(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("age requires 2 arguments")
	}
	if args[0].IsNull || args[1].IsNull {
		return NullValue(PLText{}), nil
	}
	t1, err1 := plValueToTime(args[0])
	t2, err2 := plValueToTime(args[1])
	if err1 != nil || err2 != nil {
		return PLValue{}, fmt.Errorf("age: invalid timestamp arguments")
	}
	diff := t1.Sub(t2)
	days := int(diff.Hours() / 24)
	hours := int(diff.Hours()) % 24
	minutes := int(diff.Minutes()) % 60
	seconds := int(diff.Seconds()) % 60
	result := fmt.Sprintf("%d days %02d:%02d:%02d", days, hours, minutes, seconds)
	return NewPLValue(PLText{}, result), nil
}

func builtinExtract(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("extract requires 2 arguments (field, timestamp)")
	}
	if args[0].IsNull || args[1].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	field := strings.ToLower(plValueToString(args[0]))
	t, err := plValueToTime(args[1])
	if err != nil {
		return PLValue{}, fmt.Errorf("extract: %w", err)
	}
	var result float64
	switch field {
	case "year":
		result = float64(t.Year())
	case "month":
		result = float64(t.Month())
	case "day":
		result = float64(t.Day())
	case "hour":
		result = float64(t.Hour())
	case "minute":
		result = float64(t.Minute())
	case "second":
		result = float64(t.Second())
	case "dow", "dayofweek":
		result = float64(t.Weekday())
	case "doy":
		result = float64(t.YearDay())
	case "epoch":
		result = float64(t.Unix())
	default:
		return PLValue{}, fmt.Errorf("extract: unknown field %q", field)
	}
	return NewPLValue(PLNumeric{}, result), nil
}

func builtinDateTrunc(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("date_trunc requires 2 arguments")
	}
	if args[0].IsNull || args[1].IsNull {
		return NullValue(PLTimestamp{}), nil
	}
	field := strings.ToLower(plValueToString(args[0]))
	t, err := plValueToTime(args[1])
	if err != nil {
		return PLValue{}, fmt.Errorf("date_trunc: %w", err)
	}

	loc := t.Location()
	var result time.Time
	switch field {
	case "year":
		result = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, loc)
	case "month":
		result = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc)
	case "day":
		result = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	case "hour":
		result = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc)
	case "minute":
		result = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, loc)
	case "second":
		result = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, loc)
	default:
		return PLValue{}, fmt.Errorf("date_trunc: unknown field %q", field)
	}
	return NewPLValue(PLTimestamp{}, result), nil
}

// ---------------------------------------------------------------------------
// Null functions
// ---------------------------------------------------------------------------

func (r *BuiltinRegistry) registerNullFunctions() {
	r.funcs["coalesce"] = builtinCoalesce
	r.funcs["nullif"] = builtinNullIf
	r.funcs["ifnull"] = builtinIfNull
}

func builtinCoalesce(args []PLValue) (PLValue, error) {
	for _, arg := range args {
		if !arg.IsNull {
			return arg, nil
		}
	}
	return NullValue(PLText{}), nil
}

func builtinNullIf(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("nullif requires 2 arguments")
	}
	if args[0].IsNull && args[1].IsNull {
		return NullValue(PLText{}), nil
	}
	if args[0].IsNull || args[1].IsNull {
		return args[0], nil
	}
	// If equal, return NULL.
	ls := plValueToString(args[0])
	rs := plValueToString(args[1])
	if ls == rs {
		return NullValue(args[0].Type), nil
	}
	return args[0], nil
}

func builtinIfNull(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("ifnull requires 2 arguments")
	}
	if !args[0].IsNull {
		return args[0], nil
	}
	return args[1], nil
}

// ---------------------------------------------------------------------------
// Type conversion functions
// ---------------------------------------------------------------------------

func (r *BuiltinRegistry) registerTypeFunctions() {
	r.funcs["cast"] = builtinCast
	r.funcs["to_char"] = builtinToChar
	r.funcs["to_number"] = builtinToNumber
	r.funcs["to_date"] = builtinToDate
	r.funcs["to_timestamp"] = builtinToTimestamp
}

func builtinCast(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("cast requires 2 arguments (value, type)")
	}
	if args[0].IsNull {
		return args[0], nil
	}
	typeName := plValueToString(args[1])
	target, err := ResolveType(typeName)
	if err != nil {
		return PLValue{}, err
	}
	return Coerce(args[0], target)
}

func builtinToChar(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("to_char requires at least 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	// If it's a timestamp and format is provided, format it.
	if t, ok := args[0].Value.(time.Time); ok && len(args) >= 2 {
		fmtStr := plValueToString(args[1])
		result := pgFormatToGo(fmtStr)
		return NewPLValue(PLText{}, t.Format(result)), nil
	}
	// Default: convert to text.
	coerced, err := Coerce(args[0], PLText{})
	if err != nil {
		return PLValue{}, err
	}
	return coerced, nil
}

func builtinToNumber(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("to_number requires at least 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLNumeric{}), nil
	}
	s := plValueToString(args[0])
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return PLValue{}, fmt.Errorf("to_number: %w", err)
	}
	return NewPLValue(PLNumeric{}, f), nil
}

func builtinToDate(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("to_date requires 2 arguments (text, format)")
	}
	if args[0].IsNull {
		return NullValue(PLDate{}), nil
	}
	s := plValueToString(args[0])
	fmtStr := plValueToString(args[1])
	goFmt := pgFormatToGo(fmtStr)
	t, err := time.Parse(goFmt, s)
	if err != nil {
		return PLValue{}, fmt.Errorf("to_date: %w", err)
	}
	return NewPLValue(PLDate{}, t), nil
}

func builtinToTimestamp(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("to_timestamp requires 2 arguments")
	}
	if args[0].IsNull {
		return NullValue(PLTimestamp{}), nil
	}
	s := plValueToString(args[0])
	fmtStr := plValueToString(args[1])
	goFmt := pgFormatToGo(fmtStr)
	t, err := time.Parse(goFmt, s)
	if err != nil {
		return PLValue{}, fmt.Errorf("to_timestamp: %w", err)
	}
	return NewPLValue(PLTimestamp{}, t), nil
}

// ---------------------------------------------------------------------------
// Array functions (basic)
// ---------------------------------------------------------------------------

func (r *BuiltinRegistry) registerArrayFunctions() {
	r.funcs["array_length"] = builtinArrayLength
	r.funcs["array_append"] = builtinArrayAppend
	r.funcs["unnest"] = builtinUnnest
}

func builtinArrayLength(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("array_length requires at least 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLInteger{}), nil
	}
	// Arrays are stored as []PLValue.
	if arr, ok := args[0].Value.([]PLValue); ok {
		return NewPLValue(PLInteger{}, int64(len(arr))), nil
	}
	// Or as a text representation "{a,b,c}".
	s := plValueToString(args[0])
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		inner := s[1 : len(s)-1]
		if inner == "" {
			return NewPLValue(PLInteger{}, int64(0)), nil
		}
		parts := strings.Split(inner, ",")
		return NewPLValue(PLInteger{}, int64(len(parts))), nil
	}
	return NewPLValue(PLInteger{}, int64(0)), nil
}

func builtinArrayAppend(args []PLValue) (PLValue, error) {
	if len(args) < 2 {
		return PLValue{}, fmt.Errorf("array_append requires 2 arguments")
	}
	var arr []PLValue
	if !args[0].IsNull {
		if existing, ok := args[0].Value.([]PLValue); ok {
			arr = append(arr, existing...)
		}
	}
	arr = append(arr, args[1])
	return NewPLValue(PLText{}, arr), nil
}

func builtinUnnest(args []PLValue) (PLValue, error) {
	if len(args) < 1 {
		return PLValue{}, fmt.Errorf("unnest requires 1 argument")
	}
	if args[0].IsNull {
		return NullValue(PLText{}), nil
	}
	// Basic: return the array as-is (the caller handles iteration).
	return args[0], nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// plValueToInt converts a PLValue to int64.
func plValueToInt(v PLValue) (int64, error) {
	if v.IsNull {
		return 0, fmt.Errorf("cannot convert NULL to integer")
	}
	switch val := v.Value.(type) {
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		if err != nil {
			f, ef := strconv.ParseFloat(strings.TrimSpace(val), 64)
			if ef != nil {
				return 0, fmt.Errorf("cannot convert %q to integer", val)
			}
			return int64(f), nil
		}
		return n, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to integer", val)
	}
}

// plValueToTime converts a PLValue to time.Time.
func plValueToTime(v PLValue) (time.Time, error) {
	if v.IsNull {
		return time.Time{}, fmt.Errorf("cannot convert NULL to time")
	}
	switch val := v.Value.(type) {
	case time.Time:
		return val, nil
	case string:
		// Try common formats.
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
			time.RFC3339,
		}
		for _, fmt := range formats {
			t, err := time.Parse(fmt, val)
			if err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", val)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to timestamp", val)
	}
}

// pgFormatToGo converts a PostgreSQL format string to a Go time layout string.
func pgFormatToGo(pg string) string {
	replacements := []struct{ pg, go_ string }{
		{"YYYY", "2006"},
		{"YY", "06"},
		{"MM", "01"},
		{"DD", "02"},
		{"HH24", "15"},
		{"HH12", "03"},
		{"HH", "15"},
		{"MI", "04"},
		{"SS", "05"},
		{"MS", "000"},
		{"US", "000000"},
	}
	result := pg
	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.pg, r.go_)
	}
	return result
}
