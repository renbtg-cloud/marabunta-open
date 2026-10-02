// Marabunta - Licensed under the MIT License.
package wire

import (
	"encoding/binary"

	"github.com/marabunta/marabunta-kafka/internal/broker"
)

// --------------------------------------------------------------------------
// Produce
// --------------------------------------------------------------------------

// ProduceRequest represents a decoded Kafka Produce request.
type ProduceRequest struct {
	Acks      int16
	TimeoutMS int32
	Topics    []ProduceTopicData
}

// ProduceTopicData is topic-level data in a Produce request.
type ProduceTopicData struct {
	Topic      string
	Partitions []ProducePartitionData
}

// ProducePartitionData is partition-level data in a Produce request.
type ProducePartitionData struct {
	Partition int32
	Records   [][]byte
}

// ProduceResult is the result for one topic-partition in a Produce response.
type ProduceResult struct {
	Topic        string
	Partition    int32
	ErrorCode    int16
	ErrorMessage string
	BaseOffset   int64
}

func decodeProduce(body []byte) (*ProduceRequest, error) {
	req := &ProduceRequest{}
	offset := 0

	if offset+6 > len(body) {
		return req, nil
	}

	req.Acks = readInt16(body, offset)
	offset += 2
	req.TimeoutMS = readInt32(body, offset)
	offset += 4

	// Topic count.
	if offset+4 > len(body) {
		return req, nil
	}
	topicCount := int(readInt32(body, offset))
	offset += 4

	for i := 0; i < topicCount && offset < len(body); i++ {
		var topicName string
		topicName, offset = readString(body, offset)

		td := ProduceTopicData{Topic: topicName}

		if offset+4 > len(body) {
			break
		}
		partCount := int(readInt32(body, offset))
		offset += 4

		for j := 0; j < partCount && offset < len(body); j++ {
			if offset+4 > len(body) {
				break
			}
			partition := readInt32(body, offset)
			offset += 4

			var recordSet []byte
			recordSet, offset = readBytes(body, offset)

			pd := ProducePartitionData{
				Partition: partition,
				Records:   [][]byte{recordSet},
			}
			td.Partitions = append(td.Partitions, pd)
		}
		req.Topics = append(req.Topics, td)
	}

	return req, nil
}

func encodeProduceResponse(results []ProduceResult) []byte {
	// Estimate size.
	buf := make([]byte, 0, 4+len(results)*64)

	// Topic count — group results by topic.
	topics := make(map[string][]ProduceResult)
	for _, r := range results {
		topics[r.Topic] = append(topics[r.Topic], r)
	}

	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(topics)))
	buf = append(buf, tmp...)

	for topic, partResults := range topics {
		// Topic name.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(topic)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(topic)...)

		// Partition count.
		tmp = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(partResults)))
		buf = append(buf, tmp...)

		for _, r := range partResults {
			// Partition.
			tmp = make([]byte, 4)
			binary.BigEndian.PutUint32(tmp, uint32(r.Partition))
			buf = append(buf, tmp...)

			// Error code.
			tmp = make([]byte, 2)
			binary.BigEndian.PutUint16(tmp, uint16(r.ErrorCode))
			buf = append(buf, tmp...)

			// Base offset.
			tmp = make([]byte, 8)
			binary.BigEndian.PutUint64(tmp, uint64(r.BaseOffset))
			buf = append(buf, tmp...)
		}
	}

	return buf
}

// --------------------------------------------------------------------------
// Fetch
// --------------------------------------------------------------------------

// FetchRequest represents a decoded Kafka Fetch request.
type FetchRequest struct {
	MaxWaitMS int32
	MinBytes  int32
	Topics    []FetchTopicData
}

// FetchTopicData is topic-level data in a Fetch request.
type FetchTopicData struct {
	Topic      string
	Partitions []FetchPartitionData
}

// FetchPartitionData is partition-level data in a Fetch request.
type FetchPartitionData struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

// FetchResult is the result for one topic-partition in a Fetch response.
type FetchResult struct {
	Topic         string
	Partition     int32
	ErrorCode     int16
	HighWatermark int64
	Records       [][]byte
}

func decodeFetch(body []byte) (*FetchRequest, error) {
	req := &FetchRequest{}
	offset := 0

	// ReplicaId (4) + MaxWaitMs (4) + MinBytes (4).
	if offset+12 > len(body) {
		return req, nil
	}
	offset += 4 // skip ReplicaId
	req.MaxWaitMS = readInt32(body, offset)
	offset += 4
	req.MinBytes = readInt32(body, offset)
	offset += 4

	if offset+4 > len(body) {
		return req, nil
	}
	topicCount := int(readInt32(body, offset))
	offset += 4

	for i := 0; i < topicCount && offset < len(body); i++ {
		var topicName string
		topicName, offset = readString(body, offset)

		td := FetchTopicData{Topic: topicName}

		if offset+4 > len(body) {
			break
		}
		partCount := int(readInt32(body, offset))
		offset += 4

		for j := 0; j < partCount && offset < len(body); j++ {
			if offset+16 > len(body) {
				break
			}
			partition := readInt32(body, offset)
			offset += 4
			fetchOffset := readInt64(body, offset)
			offset += 8
			maxBytes := readInt32(body, offset)
			offset += 4

			pd := FetchPartitionData{
				Partition:   partition,
				FetchOffset: fetchOffset,
				MaxBytes:    maxBytes,
			}
			td.Partitions = append(td.Partitions, pd)
		}
		req.Topics = append(req.Topics, td)
	}

	return req, nil
}

func encodeFetchResponse(results []FetchResult) []byte {
	buf := make([]byte, 0, 4+len(results)*128)

	topics := make(map[string][]FetchResult)
	for _, r := range results {
		topics[r.Topic] = append(topics[r.Topic], r)
	}

	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(topics)))
	buf = append(buf, tmp...)

	for topic, partResults := range topics {
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(topic)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(topic)...)

		tmp = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(partResults)))
		buf = append(buf, tmp...)

		for _, r := range partResults {
			tmp = make([]byte, 4)
			binary.BigEndian.PutUint32(tmp, uint32(r.Partition))
			buf = append(buf, tmp...)

			tmp = make([]byte, 2)
			binary.BigEndian.PutUint16(tmp, uint16(r.ErrorCode))
			buf = append(buf, tmp...)

			tmp = make([]byte, 8)
			binary.BigEndian.PutUint64(tmp, uint64(r.HighWatermark))
			buf = append(buf, tmp...)

			// Record set bytes.
			var recordBytes []byte
			for _, rec := range r.Records {
				recordBytes = append(recordBytes, rec...)
			}
			tmp = make([]byte, 4)
			binary.BigEndian.PutUint32(tmp, uint32(len(recordBytes)))
			buf = append(buf, tmp...)
			buf = append(buf, recordBytes...)
		}
	}

	return buf
}

// --------------------------------------------------------------------------
// Metadata
// --------------------------------------------------------------------------

func encodeMetadataResponse(meta *broker.ClusterMetadata) []byte {
	buf := make([]byte, 0, 256)

	// Broker count.
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(meta.Brokers)))
	buf = append(buf, tmp...)

	for _, b := range meta.Brokers {
		// Node ID.
		tmp = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(b.NodeID))
		buf = append(buf, tmp...)

		// Host.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(b.Host)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(b.Host)...)

		// Port.
		tmp = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(b.Port))
		buf = append(buf, tmp...)
	}

	// Topic count.
	tmp = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(meta.Topics)))
	buf = append(buf, tmp...)

	for _, t := range meta.Topics {
		// Error code.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, 0)
		buf = append(buf, tmp...)

		// Topic name.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(t.Name)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(t.Name)...)

		// Is internal.
		buf = append(buf, 0)

		// Partition count.
		tmp = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(t.PartitionCount))
		buf = append(buf, tmp...)
	}

	return buf
}

func encodeFindCoordinatorResponse(addr string) []byte {
	buf := make([]byte, 0, 64)

	// Error code.
	tmp := make([]byte, 2)
	binary.BigEndian.PutUint16(tmp, 0)
	buf = append(buf, tmp...)

	// Node ID (coordinator's broker ID; 0 for single-node).
	tmp = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, 0)
	buf = append(buf, tmp...)

	// Host.
	tmp = make([]byte, 2)
	binary.BigEndian.PutUint16(tmp, uint16(len(addr)))
	buf = append(buf, tmp...)
	buf = append(buf, []byte(addr)...)

	// Port.
	tmp = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, 9092)
	buf = append(buf, tmp...)

	return buf
}

func encodeAPIVersionsResponse() []byte {
	apiVersions := []struct {
		key    int16
		minVer int16
		maxVer int16
	}{
		{APIKeyProduce, 0, 8},
		{APIKeyFetch, 0, 11},
		{APIKeyListOffsets, 0, 5},
		{APIKeyMetadata, 0, 9},
		{APIKeyOffsetCommit, 0, 7},
		{APIKeyOffsetFetch, 0, 5},
		{APIKeyFindCoordinator, 0, 2},
		{APIKeyJoinGroup, 0, 5},
		{APIKeySyncGroup, 0, 3},
		{APIKeyHeartbeat, 0, 3},
		{APIKeyLeaveGroup, 0, 2},
		{APIKeyCreateTopics, 0, 3},
		{APIKeyDeleteTopics, 0, 3},
		{APIKeyAPIVersions, 0, 2},
	}

	buf := make([]byte, 0, 6+len(apiVersions)*6)

	// Error code.
	tmp := make([]byte, 2)
	binary.BigEndian.PutUint16(tmp, 0)
	buf = append(buf, tmp...)

	// API count.
	tmp = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(apiVersions)))
	buf = append(buf, tmp...)

	for _, av := range apiVersions {
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(av.key))
		buf = append(buf, tmp...)

		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(av.minVer))
		buf = append(buf, tmp...)

		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(av.maxVer))
		buf = append(buf, tmp...)
	}

	return buf
}

func encodeErrorResponse() []byte {
	// Generic error response: just an error code.
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, 1) // UNKNOWN_SERVER_ERROR
	return buf
}
