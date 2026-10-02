// Marabunta - Licensed under the MIT License.
package wire

import (
	"encoding/binary"
)

// Kafka API keys.
const (
	APIKeyProduce         int16 = 0
	APIKeyFetch           int16 = 1
	APIKeyListOffsets     int16 = 2
	APIKeyMetadata        int16 = 3
	APIKeyOffsetCommit    int16 = 8
	APIKeyOffsetFetch     int16 = 9
	APIKeyFindCoordinator int16 = 10
	APIKeyJoinGroup       int16 = 11
	APIKeySyncGroup       int16 = 14
	APIKeyHeartbeat       int16 = 12
	APIKeyLeaveGroup      int16 = 13
	APIKeyCreateTopics    int16 = 19
	APIKeyDeleteTopics    int16 = 20
	APIKeyAPIVersions     int16 = 18
)

// KafkaRequest represents a parsed Kafka request.
type KafkaRequest struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
	Body          []byte
}

// KafkaResponse represents a Kafka response to be sent.
type KafkaResponse struct {
	CorrelationID int32
	Body          []byte
}

// readInt16 reads a big-endian int16 from buf at the given offset.
func readInt16(buf []byte, offset int) int16 {
	return int16(binary.BigEndian.Uint16(buf[offset:]))
}

// readInt32 reads a big-endian int32 from buf at the given offset.
func readInt32(buf []byte, offset int) int32 {
	return int32(binary.BigEndian.Uint32(buf[offset:]))
}

// readInt64 reads a big-endian int64 from buf at the given offset.
func readInt64(buf []byte, offset int) int64 {
	return int64(binary.BigEndian.Uint64(buf[offset:]))
}

// readString reads a Kafka-encoded string (2-byte length + UTF-8 bytes).
// A length of 0xFFFF (-1 as int16) represents a null string.
func readString(buf []byte, offset int) (string, int) {
	if offset+2 > len(buf) {
		return "", offset
	}
	rawLen := int16(binary.BigEndian.Uint16(buf[offset:]))
	offset += 2
	if rawLen <= 0 || offset+int(rawLen) > len(buf) {
		return "", offset
	}
	length := int(rawLen)
	s := string(buf[offset : offset+length])
	return s, offset + length
}

// writeString writes a Kafka-encoded string to buf.
func writeString(buf []byte, offset int, s string) int {
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(s)))
	offset += 2
	copy(buf[offset:], s)
	return offset + len(s)
}

// readBytes reads a Kafka-encoded byte array (4-byte length + bytes).
// A length of -1 (0xFFFFFFFF) represents null bytes per the Kafka protocol.
func readBytes(buf []byte, offset int) ([]byte, int) {
	if offset+4 > len(buf) {
		return nil, offset
	}
	rawLen := int32(binary.BigEndian.Uint32(buf[offset:]))
	offset += 4
	if rawLen < 0 {
		return nil, offset
	}
	length := int(rawLen)
	if offset+length > len(buf) {
		return nil, offset
	}
	result := make([]byte, length)
	copy(result, buf[offset:offset+length])
	return result, offset + length
}
