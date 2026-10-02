// Marabunta - Licensed under the MIT License.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/marabunta/marabunta-postgres/internal/types"
)

// writeAuthOk sends an AuthenticationOk message (R, length=8, status=0).
func (c *Conn) writeAuthOk() {
	c.writer.WriteByte('R')
	writeInt32(c.writer, 8) // length
	writeInt32(c.writer, 0) // auth ok
}

// writeAuthMD5Password sends an AuthenticationMD5Password message (R,
// length=12, auth-code=5, 4-byte salt) requesting the client provide an
// MD5-hashed password.
func (c *Conn) writeAuthMD5Password(salt []byte) {
	c.writer.WriteByte('R')
	writeInt32(c.writer, 12) // length: 4 (len) + 4 (auth code) + 4 (salt)
	writeInt32(c.writer, 5)  // auth code 5 = MD5
	c.writer.Write(salt[:4])
}

// readPasswordMessage reads a PasswordMessage ('p') from the client and
// returns the password string (without the null terminator).
func (c *Conn) readPasswordMessage() (string, error) {
	msgType, err := c.reader.ReadByte()
	if err != nil {
		return "", fmt.Errorf("read password message type: %w", err)
	}
	if msgType != 'p' {
		return "", fmt.Errorf("expected PasswordMessage ('p'), got '%c'", msgType)
	}

	var length int32
	if err := binary.Read(c.reader, binary.BigEndian, &length); err != nil {
		return "", fmt.Errorf("read password message length: %w", err)
	}
	if length < 4 {
		return "", fmt.Errorf("invalid password message length: %d", length)
	}

	body := make([]byte, length-4)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return "", fmt.Errorf("read password message body: %w", err)
	}

	// Strip trailing null terminator.
	password := strings.TrimRight(string(body), "\x00")
	return password, nil
}

// writeParamStatus sends a ParameterStatus message.
func (c *Conn) writeParamStatus(name, value string) {
	length := 4 + len(name) + 1 + len(value) + 1
	c.writer.WriteByte('S')
	writeInt32(c.writer, int32(length))
	c.writer.WriteString(name)
	c.writer.WriteByte(0)
	c.writer.WriteString(value)
	c.writer.WriteByte(0)
}

// writeBackendKeyData sends the BackendKeyData message.
func (c *Conn) writeBackendKeyData(processID, secretKey int32) {
	c.writer.WriteByte('K')
	writeInt32(c.writer, 12) // length
	writeInt32(c.writer, processID)
	writeInt32(c.writer, secretKey)
}

// writeReadyForQuery sends a ReadyForQuery message.
// status: 'I' = idle, 'T' = in transaction, 'E' = failed transaction.
func (c *Conn) writeReadyForQuery(status byte) {
	c.writer.WriteByte('Z')
	writeInt32(c.writer, 5) // length
	c.writer.WriteByte(status)
}

// writeRowDescription sends a RowDescription message.
func (c *Conn) writeRowDescription(cols []types.ColumnDef) {
	// Calculate total length.
	bodyLen := 2 // field count (int16)
	for _, col := range cols {
		bodyLen += len(col.Name) + 1 // name + null
		bodyLen += 4                 // table OID
		bodyLen += 2                 // column attr number
		bodyLen += 4                 // type OID
		bodyLen += 2                 // type size
		bodyLen += 4                 // type modifier
		bodyLen += 2                 // format code
	}

	c.writer.WriteByte('T')
	writeInt32(c.writer, int32(bodyLen+4)) // +4 for length field itself
	writeInt16(c.writer, int16(len(cols)))

	for _, col := range cols {
		c.writer.WriteString(col.Name)
		c.writer.WriteByte(0)
		writeInt32(c.writer, 0)          // table OID
		writeInt16(c.writer, 0)          // column attribute number
		writeInt32(c.writer, col.OID)    // type OID
		writeInt16(c.writer, col.TypeSize)
		writeInt32(c.writer, col.TypeMod)
		writeInt16(c.writer, col.Format)
	}
}

// writeDataRow sends a DataRow message. Each value is a text-encoded string,
// or nil for NULL.
func (c *Conn) writeDataRow(values [][]byte) {
	bodyLen := 2 // column count
	for _, v := range values {
		if v == nil {
			bodyLen += 4 // -1 for NULL
		} else {
			bodyLen += 4 + len(v) // length + data
		}
	}

	c.writer.WriteByte('D')
	writeInt32(c.writer, int32(bodyLen+4))
	writeInt16(c.writer, int16(len(values)))

	for _, v := range values {
		if v == nil {
			writeInt32(c.writer, -1)
		} else {
			writeInt32(c.writer, int32(len(v)))
			c.writer.Write(v)
		}
	}
}

// writeCommandComplete sends a CommandComplete message.
func (c *Conn) writeCommandComplete(tag string) {
	length := 4 + len(tag) + 1
	c.writer.WriteByte('C')
	writeInt32(c.writer, int32(length))
	c.writer.WriteString(tag)
	c.writer.WriteByte(0)
}

// writeError sends an ErrorResponse message.
func (c *Conn) writeError(severity, code, message string) {
	// Calculate body length.
	bodyLen := 0
	bodyLen += 1 + len(severity) + 1  // 'S' field
	bodyLen += 1 + len(severity) + 1  // 'V' field (non-localized severity)
	bodyLen += 1 + len(code) + 1      // 'C' field
	bodyLen += 1 + len(message) + 1   // 'M' field
	bodyLen += 1                       // terminator

	c.writer.WriteByte('E')
	writeInt32(c.writer, int32(bodyLen+4))

	// Severity (localized).
	c.writer.WriteByte('S')
	c.writer.WriteString(severity)
	c.writer.WriteByte(0)

	// Severity (non-localized).
	c.writer.WriteByte('V')
	c.writer.WriteString(severity)
	c.writer.WriteByte(0)

	// SQLSTATE code.
	c.writer.WriteByte('C')
	c.writer.WriteString(code)
	c.writer.WriteByte(0)

	// Message.
	c.writer.WriteByte('M')
	c.writer.WriteString(message)
	c.writer.WriteByte(0)

	// Terminator.
	c.writer.WriteByte(0)
}

// writeNotice sends a NoticeResponse message (pgwire message type 'N').
// This is used to relay RAISE NOTICE/INFO/WARNING messages from PL/pgSQL
// back to the client. The format mirrors ErrorResponse but uses 'N' as the
// message type byte.
func (c *Conn) writeNotice(severity, message string) {
	// Map severity to a SQLSTATE code for notices.
	code := "00000" // success
	switch strings.ToUpper(severity) {
	case "WARNING":
		code = "01000"
	case "NOTICE":
		code = "00000"
	case "INFO":
		code = "00000"
	case "LOG":
		code = "00000"
	case "DEBUG":
		code = "00000"
	}

	// Calculate body length.
	bodyLen := 0
	bodyLen += 1 + len(severity) + 1 // 'S' field
	bodyLen += 1 + len(severity) + 1 // 'V' field (non-localized severity)
	bodyLen += 1 + len(code) + 1     // 'C' field
	bodyLen += 1 + len(message) + 1  // 'M' field
	bodyLen += 1                     // terminator

	c.writer.WriteByte('N') // NoticeResponse
	writeInt32(c.writer, int32(bodyLen+4))

	// Severity (localized).
	c.writer.WriteByte('S')
	c.writer.WriteString(severity)
	c.writer.WriteByte(0)

	// Severity (non-localized).
	c.writer.WriteByte('V')
	c.writer.WriteString(severity)
	c.writer.WriteByte(0)

	// SQLSTATE code.
	c.writer.WriteByte('C')
	c.writer.WriteString(code)
	c.writer.WriteByte(0)

	// Message.
	c.writer.WriteByte('M')
	c.writer.WriteString(message)
	c.writer.WriteByte(0)

	// Terminator.
	c.writer.WriteByte(0)
}

// writeEmptyQuery sends an EmptyQueryResponse.
func (c *Conn) writeEmptyQuery() {
	c.writer.WriteByte('I')
	writeInt32(c.writer, 4)
}

// writeParseComplete sends a ParseComplete message.
func (c *Conn) writeParseComplete() {
	c.writer.WriteByte('1')
	writeInt32(c.writer, 4)
}

// writeBindComplete sends a BindComplete message.
func (c *Conn) writeBindComplete() {
	c.writer.WriteByte('2')
	writeInt32(c.writer, 4)
}

// writeCloseComplete sends a CloseComplete message.
func (c *Conn) writeCloseComplete() {
	c.writer.WriteByte('3')
	writeInt32(c.writer, 4)
}

// writeNoData sends a NoData message.
func (c *Conn) writeNoData() {
	c.writer.WriteByte('n')
	writeInt32(c.writer, 4)
}

// writeInt32 writes a big-endian int32 to the writer.
func writeInt32(w io.Writer, v int32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	w.Write(buf[:])
}

// writeInt16 writes a big-endian int16 to the writer.
func writeInt16(w io.Writer, v int16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(v))
	w.Write(buf[:])
}
