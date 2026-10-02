// Marabunta - Licensed under the MIT License.
package terminal

import (
	"fmt"
	"sync"
)

// BMSMap represents a Basic Mapping Support screen definition (spec C.4).
// BMS maps define the layout of 3270 screens with named fields.
type BMSMap struct {
	Name   string
	Rows   int
	Cols   int
	Fields []BMSField
}

// BMSField represents a single field in a BMS map.
type BMSField struct {
	Name      string
	Row       int
	Col       int
	Length    int
	Attr      byte // field attribute
	Initial   string // initial value
	Protected bool
	Hidden    bool
	Bright    bool
}

// BMSCache caches compiled BMS maps for fast rendering (spec C.4).
type BMSCache struct {
	mu   sync.RWMutex
	maps map[string]*CompiledBMS
}

// CompiledBMS is a pre-rendered BMS map ready for fast screen updates.
type CompiledBMS struct {
	Name   string
	Stream []byte // pre-built 3270 data stream (without data field values)
	Fields []fieldSlot
}

// fieldSlot marks where a dynamic field value should be inserted.
type fieldSlot struct {
	name   string
	offset int // byte offset in the stream
	length int // maximum field length
}

// NewBMSCache creates a new BMS map cache.
func NewBMSCache() *BMSCache {
	return &BMSCache{
		maps: make(map[string]*CompiledBMS),
	}
}

// Compile compiles a BMS map definition into a pre-rendered data stream.
func (c *BMSCache) Compile(bms *BMSMap) *CompiledBMS {
	c.mu.Lock()
	defer c.mu.Unlock()

	compiled := compileBMS(bms)
	c.maps[bms.Name] = compiled
	return compiled
}

// Get retrieves a compiled BMS map from the cache.
func (c *BMSCache) Get(name string) *CompiledBMS {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maps[name]
}

// Render generates a complete 3270 data stream from a compiled BMS map
// with the given field values. Returns the stream with IAC EOR framing.
func (c *BMSCache) Render(name string, values map[string]string) ([]byte, error) {
	compiled := c.Get(name)
	if compiled == nil {
		return nil, fmt.Errorf("BMS map %q not found", name)
	}
	return renderBMS(compiled, values), nil
}

// compileBMS pre-builds the 3270 data stream for a BMS map.
func compileBMS(bms *BMSMap) *CompiledBMS {
	ds := NewDataStream()
	ds.EraseWrite(WCCReset | WCCUnlock)

	compiled := &CompiledBMS{
		Name: bms.Name,
	}

	for _, field := range bms.Fields {
		// Position.
		ds.SBA(field.Row, field.Col)

		// Start field with attribute.
		attr := byte(0x00)
		if field.Protected {
			attr |= FAProtected
		}
		if field.Hidden {
			attr |= FAHidden
		}
		if field.Bright {
			attr |= FAHighIntensity
		}
		ds.SF(attr)

		// Record slot position for dynamic values.
		slot := fieldSlot{
			name:   field.Name,
			offset: len(ds.buf),
			length: field.Length,
		}
		compiled.Fields = append(compiled.Fields, slot)

		// Write initial value or spaces as placeholder.
		if field.Initial != "" {
			ds.Text(padRight(field.Initial, field.Length))
		} else {
			ds.Text(padRight("", field.Length))
		}
	}

	compiled.Stream = ds.Bytes()
	return compiled
}

// renderBMS creates a complete screen from a compiled BMS map with values.
func renderBMS(compiled *CompiledBMS, values map[string]string) []byte {
	// Copy the base stream.
	stream := make([]byte, len(compiled.Stream))
	copy(stream, compiled.Stream)

	// Fill in field values.
	for _, slot := range compiled.Fields {
		value, ok := values[slot.name]
		if !ok {
			continue
		}

		// Pad or truncate value to field length.
		encoded := ebcdicEncode(padRight(value, slot.length))
		if slot.offset+len(encoded) <= len(stream) {
			copy(stream[slot.offset:], encoded[:slot.length])
		}
	}

	// Add IAC EOR framing.
	framed := make([]byte, len(stream)+2)
	copy(framed, stream)
	framed[len(stream)] = TelnetIAC
	framed[len(stream)+1] = TelnetEOR
	return framed
}

// padRight pads a string to the given length with spaces.
func padRight(s string, length int) string {
	if len(s) >= length {
		return s[:length]
	}
	padding := make([]byte, length-len(s))
	for i := range padding {
		padding[i] = ' '
	}
	return s + string(padding)
}

// CacheSize returns the number of cached BMS maps.
func (c *BMSCache) CacheSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.maps)
}
