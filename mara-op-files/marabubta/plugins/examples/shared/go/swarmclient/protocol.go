// Marabunta - Licensed under the MIT License.
package swarmclient

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxMessageSize is the maximum allowed wire message size (16 MB).
const MaxMessageSize = 16 * 1024 * 1024

// HeaderSize is the byte length of the big-endian uint32 length prefix.
const HeaderSize = 4

// ReadMessage reads a single length-delimited JSON message from r.
// The wire format is: [4 bytes big-endian length] [JSON body of that length].
// Returns ErrMessageTooLarge if the declared length exceeds MaxMessageSize.
func ReadMessage(r io.Reader) ([]byte, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	length := binary.BigEndian.Uint32(header)
	if length == 0 {
		return nil, fmt.Errorf("empty message (length=0)")
	}
	if length > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes (max %d)", length, MaxMessageSize)
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("read body (%d bytes): %w", length, err)
	}
	return body, nil
}

// WriteMessage writes a single length-delimited JSON message to w.
// The wire format is: [4 bytes big-endian length] [JSON body of that length].
// The header and body are written in a single Write call to prevent partial
// frames if the connection breaks or a deadline fires between writes.
func WriteMessage(w io.Writer, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty message (length=0)")
	}
	if len(data) > MaxMessageSize {
		return fmt.Errorf("message too large: %d bytes (max %d)", len(data), MaxMessageSize)
	}

	frame := make([]byte, HeaderSize+len(data))
	binary.BigEndian.PutUint32(frame[:HeaderSize], uint32(len(data)))
	copy(frame[HeaderSize:], data)

	if _, err := w.Write(frame); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}

// ReadWireMessage reads and decodes a WireMessage from the reader.
func ReadWireMessage(r io.Reader) (*WireMessage, error) {
	data, err := ReadMessage(r)
	if err != nil {
		return nil, err
	}
	var msg WireMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal wire message: %w", err)
	}
	return &msg, nil
}

// WriteWireMessage encodes and writes a WireMessage to the writer.
func WriteWireMessage(w io.Writer, msg *WireMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal wire message: %w", err)
	}
	return WriteMessage(w, data)
}

// EncodePayload marshals a typed payload into a WireMessage ready for sending.
func EncodePayload(msgType string, payload interface{}) (*WireMessage, error) {
	return &WireMessage{
		Type:    msgType,
		Payload: payload,
	}, nil
}

// DecodePayload extracts and unmarshals the payload from a WireMessage into dst.
// Because json.Unmarshal stores the payload as a map/slice when the target is
// interface{}, we re-marshal the payload then unmarshal into the concrete type.
func DecodePayload(msg *WireMessage, dst interface{}) error {
	raw, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("re-marshal payload: %w", err)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("unmarshal payload into %T: %w", dst, err)
	}
	return nil
}
