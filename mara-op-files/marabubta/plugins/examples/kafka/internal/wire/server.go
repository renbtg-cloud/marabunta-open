// Marabunta - Licensed under the MIT License.
// Package wire implements the Kafka binary protocol listener.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marabunta/marabunta-kafka/internal/broker"
	"github.com/marabunta/marabunta-kafka/internal/consumer"
	"github.com/marabunta/marabunta-kafka/internal/storage"
)

// Server is a Kafka binary protocol TCP server.
type Server struct {
	addr      string
	listener  net.Listener
	broker    *broker.Broker
	logStore  *storage.LogStore
	consumer  *consumer.Coordinator
	offsets   *storage.OffsetManager
	closed    atomic.Bool
	wg        sync.WaitGroup
}

// NewServer creates a new Kafka protocol server.
func NewServer(addr string, brk *broker.Broker, logStore *storage.LogStore, cons *consumer.Coordinator, offsets *storage.OffsetManager) *Server {
	return &Server{
		addr:     addr,
		broker:   brk,
		logStore: logStore,
		consumer: cons,
		offsets:  offsets,
	}
}

// ListenAndServe starts the Kafka listener and accepts connections.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("kafka listen: %w", err)
	}
	s.listener = ln

	for {
		if s.closed.Load() {
			return nil
		}
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			log.Printf("kafka: accept error: %v", err)
			continue
		}
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// Close shuts down the server.
func (s *Server) Close() {
	s.closed.Store(true)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		if s.closed.Load() {
			return
		}

		// Read Kafka request header: Size (4 bytes) + ApiKey (2) + ApiVersion (2) + CorrelationId (4).
		req, err := readKafkaRequest(conn)
		if err != nil {
			if err != io.EOF && !s.closed.Load() {
				log.Printf("kafka: read request error: %v", err)
			}
			return
		}

		resp, err := s.handleRequest(req)
		if err != nil {
			log.Printf("kafka: handle request error (api=%d): %v", req.APIKey, err)
			resp = &KafkaResponse{
				CorrelationID: req.CorrelationID,
				Body:          encodeErrorResponse(),
			}
		}

		if err := writeKafkaResponse(conn, resp); err != nil {
			log.Printf("kafka: write response error: %v", err)
			return
		}
	}
}

func (s *Server) handleRequest(req *KafkaRequest) (*KafkaResponse, error) {
	switch req.APIKey {
	case APIKeyProduce:
		return s.handleProduce(req)
	case APIKeyFetch:
		return s.handleFetch(req)
	case APIKeyMetadata:
		return s.handleMetadata(req)
	case APIKeyOffsetCommit:
		return s.handleOffsetCommit(req)
	case APIKeyOffsetFetch:
		return s.handleOffsetFetch(req)
	case APIKeyFindCoordinator:
		return s.handleFindCoordinator(req)
	case APIKeyJoinGroup:
		return s.handleJoinGroup(req)
	case APIKeySyncGroup:
		return s.handleSyncGroup(req)
	case APIKeyHeartbeat:
		return s.handleHeartbeat(req)
	case APIKeyLeaveGroup:
		return s.handleLeaveGroup(req)
	case APIKeyListOffsets:
		return s.handleListOffsets(req)
	case APIKeyCreateTopics:
		return s.handleCreateTopics(req)
	case APIKeyDeleteTopics:
		return s.handleDeleteTopics(req)
	case APIKeyAPIVersions:
		return s.handleAPIVersions(req)
	default:
		return nil, fmt.Errorf("unsupported API key: %d", req.APIKey)
	}
}

func (s *Server) handleProduce(req *KafkaRequest) (*KafkaResponse, error) {
	produce, err := decodeProduce(req.Body)
	if err != nil {
		return nil, fmt.Errorf("decode produce: %w", err)
	}

	var results []ProduceResult
	for _, topicData := range produce.Topics {
		for _, partData := range topicData.Partitions {
			offset, err := s.broker.Produce(topicData.Topic, partData.Partition, partData.Records)
			result := ProduceResult{
				Topic:     topicData.Topic,
				Partition: partData.Partition,
			}
			if err != nil {
				result.ErrorCode = 1 // UNKNOWN_SERVER_ERROR
				result.ErrorMessage = err.Error()
			} else {
				result.BaseOffset = offset
			}
			results = append(results, result)
		}
	}

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          encodeProduceResponse(results),
	}, nil
}

func (s *Server) handleFetch(req *KafkaRequest) (*KafkaResponse, error) {
	fetch, err := decodeFetch(req.Body)
	if err != nil {
		return nil, fmt.Errorf("decode fetch: %w", err)
	}

	var results []FetchResult
	for _, topicData := range fetch.Topics {
		for _, partData := range topicData.Partitions {
			records, highWatermark, err := s.broker.Fetch(topicData.Topic, partData.Partition, partData.FetchOffset, partData.MaxBytes)
			result := FetchResult{
				Topic:         topicData.Topic,
				Partition:     partData.Partition,
				HighWatermark: highWatermark,
			}
			if err != nil {
				result.ErrorCode = 1
			} else {
				result.Records = records
			}
			results = append(results, result)
		}
	}

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          encodeFetchResponse(results),
	}, nil
}

func (s *Server) handleMetadata(req *KafkaRequest) (*KafkaResponse, error) {
	meta := s.broker.GetMetadata()
	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          encodeMetadataResponse(meta),
	}, nil
}

func (s *Server) handleOffsetCommit(req *KafkaRequest) (*KafkaResponse, error) {
	body := req.Body
	offset := 0

	// Decode: GroupID (string), GenerationID (int32), MemberID (string).
	var groupID string
	groupID, offset = readString(body, offset)
	if offset+4 > len(body) {
		return &KafkaResponse{CorrelationID: req.CorrelationID, Body: make([]byte, 4)}, nil
	}
	offset += 4 // skip GenerationID
	_, offset = readString(body, offset) // skip MemberID

	// Topics array.
	type partCommit struct {
		topic     string
		partition int32
	}
	var committed []partCommit

	if offset+4 > len(body) {
		return &KafkaResponse{CorrelationID: req.CorrelationID, Body: make([]byte, 4)}, nil
	}
	topicCount := int(readInt32(body, offset))
	offset += 4

	for i := 0; i < topicCount && offset < len(body); i++ {
		var topicName string
		topicName, offset = readString(body, offset)

		if offset+4 > len(body) {
			break
		}
		partCount := int(readInt32(body, offset))
		offset += 4

		for j := 0; j < partCount && offset < len(body); j++ {
			if offset+14 > len(body) { // partition(4) + offset(8) + metadata_len(2)
				break
			}
			partition := readInt32(body, offset)
			offset += 4
			committedOffset := readInt64(body, offset)
			offset += 8
			_, offset = readString(body, offset) // skip metadata

			s.offsets.Commit(topicName, groupID, partition, committedOffset)
			committed = append(committed, partCommit{topic: topicName, partition: partition})
		}
	}

	// Encode response: group by topic.
	topics := make(map[string][]int32)
	for _, c := range committed {
		topics[c.topic] = append(topics[c.topic], c.partition)
	}

	buf := make([]byte, 0, 4+len(committed)*8)
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(topics)))
	buf = append(buf, tmp...)

	for topicName, partitions := range topics {
		// Topic name.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(topicName)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(topicName)...)

		// Partition count.
		tmp = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(partitions)))
		buf = append(buf, tmp...)

		for _, p := range partitions {
			// Partition ID.
			tmp = make([]byte, 4)
			binary.BigEndian.PutUint32(tmp, uint32(p))
			buf = append(buf, tmp...)

			// Error code (0 = no error).
			tmp = make([]byte, 2)
			binary.BigEndian.PutUint16(tmp, 0)
			buf = append(buf, tmp...)
		}
	}

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleOffsetFetch(req *KafkaRequest) (*KafkaResponse, error) {
	body := req.Body
	offset := 0

	// Decode: GroupID (string).
	var groupID string
	groupID, offset = readString(body, offset)

	// Decode topics array.
	type topicPartitions struct {
		name       string
		partitions []int32
	}
	var requested []topicPartitions

	if offset+4 > len(body) {
		return &KafkaResponse{CorrelationID: req.CorrelationID, Body: make([]byte, 4)}, nil
	}
	topicCount := int(readInt32(body, offset))
	offset += 4

	for i := 0; i < topicCount && offset < len(body); i++ {
		var topicName string
		topicName, offset = readString(body, offset)

		tp := topicPartitions{name: topicName}

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
			tp.partitions = append(tp.partitions, partition)
		}
		requested = append(requested, tp)
	}

	// Encode response.
	buf := make([]byte, 0, 4+len(requested)*64)
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(requested)))
	buf = append(buf, tmp...)

	for _, tp := range requested {
		// Topic name.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(tp.name)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(tp.name)...)

		// Partition count.
		tmp = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(tp.partitions)))
		buf = append(buf, tmp...)

		for _, p := range tp.partitions {
			committedOffset := s.offsets.Fetch(tp.name, groupID, p)

			// Partition ID.
			tmp = make([]byte, 4)
			binary.BigEndian.PutUint32(tmp, uint32(p))
			buf = append(buf, tmp...)

			// Committed offset.
			tmp = make([]byte, 8)
			binary.BigEndian.PutUint64(tmp, uint64(committedOffset))
			buf = append(buf, tmp...)

			// Metadata (empty string).
			tmp = make([]byte, 2)
			binary.BigEndian.PutUint16(tmp, 0)
			buf = append(buf, tmp...)

			// Error code (0 = no error).
			tmp = make([]byte, 2)
			binary.BigEndian.PutUint16(tmp, 0)
			buf = append(buf, tmp...)
		}
	}

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleFindCoordinator(req *KafkaRequest) (*KafkaResponse, error) {
	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          encodeFindCoordinatorResponse(s.addr),
	}, nil
}

func (s *Server) handleJoinGroup(req *KafkaRequest) (*KafkaResponse, error) {
	body := req.Body
	offset := 0

	// Decode: GroupID (string), SessionTimeoutMs (int32), MemberID (string),
	// ProtocolType (string), Protocols array.
	var groupID string
	groupID, offset = readString(body, offset)
	if offset+4 > len(body) {
		return nil, fmt.Errorf("join group: body too short for SessionTimeoutMs")
	}
	offset += 4 // skip SessionTimeoutMs
	var memberID string
	memberID, offset = readString(body, offset)
	var protocolType string
	protocolType, offset = readString(body, offset)

	// Parse protocols array to extract subscribed topics.
	var topics []string
	var protocolName string
	if offset+4 <= len(body) {
		protocolCount := int(readInt32(body, offset))
		offset += 4
		for i := 0; i < protocolCount && offset < len(body); i++ {
			var pName string
			pName, offset = readString(body, offset)
			if i == 0 {
				protocolName = pName
			}
			// Protocol metadata is a bytes field containing subscription info.
			// For the "consumer" protocol type, it encodes:
			//   Version (int16), Topics array (int32 count + strings), UserData (bytes).
			var metaBytes []byte
			metaBytes, offset = readBytes(body, offset)
			if i == 0 && len(metaBytes) >= 6 {
				// Parse subscription metadata from the first protocol entry.
				mOff := 2 // skip Version (int16)
				if mOff+4 <= len(metaBytes) {
					topicCount := int(readInt32(metaBytes, mOff))
					mOff += 4
					for t := 0; t < topicCount && mOff < len(metaBytes); t++ {
						var topicName string
						topicName, mOff = readString(metaBytes, mOff)
						if topicName != "" {
							topics = append(topics, topicName)
						}
					}
				}
			}
		}
	}
	// Suppress unused variable warning for protocolType.
	_ = protocolType

	// If memberID is empty, generate one.
	if memberID == "" {
		memberID = fmt.Sprintf("consumer-%s-%d", groupID, time.Now().UnixNano())
	}

	generation, err := s.consumer.JoinGroup(groupID, memberID, req.ClientID, topics)
	if err != nil {
		return nil, fmt.Errorf("join group: %w", err)
	}

	// Get the leader from the consumer coordinator state.
	// For simplicity, the first member to join becomes the leader.
	leader := memberID

	if protocolName == "" {
		protocolName = "range"
	}

	// Encode response: error_code(int16) + generation(int32) + protocol(string)
	// + leader(string) + memberID(string) + members array.
	buf := make([]byte, 0, 64)

	// Error code (0 = no error).
	tmp := make([]byte, 2)
	binary.BigEndian.PutUint16(tmp, 0)
	buf = append(buf, tmp...)

	// Generation ID.
	tmp = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(generation))
	buf = append(buf, tmp...)

	// Protocol name.
	tmp = make([]byte, 2+len(protocolName))
	writeString(tmp, 0, protocolName)
	buf = append(buf, tmp...)

	// Leader.
	tmp = make([]byte, 2+len(leader))
	writeString(tmp, 0, leader)
	buf = append(buf, tmp...)

	// Member ID.
	tmp = make([]byte, 2+len(memberID))
	writeString(tmp, 0, memberID)
	buf = append(buf, tmp...)

	// Members array (required by Kafka protocol; at minimum includes this member).
	// Count.
	tmp = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, 1) // single member (self)
	buf = append(buf, tmp...)

	// Member entry: memberID (string) + metadata (bytes).
	tmp = make([]byte, 2+len(memberID))
	writeString(tmp, 0, memberID)
	buf = append(buf, tmp...)

	// Empty metadata bytes (length = 0).
	tmp = make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, 0)
	buf = append(buf, tmp...)

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleSyncGroup(req *KafkaRequest) (*KafkaResponse, error) {
	// SyncGroup: return error_code=0 and empty assignment bytes.
	// Decode is not strictly needed since we return an empty assignment,
	// but we consume the body for completeness.
	buf := make([]byte, 6)

	// Error code (0 = no error).
	binary.BigEndian.PutUint16(buf[0:2], 0)

	// Assignment bytes (empty, 4-byte length = 0).
	binary.BigEndian.PutUint32(buf[2:6], 0)

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleHeartbeat(req *KafkaRequest) (*KafkaResponse, error) {
	body := req.Body
	offset := 0

	// Decode: GroupID (string), GenerationID (int32), MemberID (string).
	var groupID string
	groupID, offset = readString(body, offset)
	offset += 4 // skip GenerationID
	var memberID string
	memberID, offset = readString(body, offset)
	_ = offset // consumed

	var errorCode int16
	if err := s.consumer.Heartbeat(groupID, memberID); err != nil {
		errorCode = 26 // REBALANCE_IN_PROGRESS as fallback error
	}

	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(errorCode))

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleLeaveGroup(req *KafkaRequest) (*KafkaResponse, error) {
	body := req.Body
	offset := 0

	// Decode: GroupID (string), MemberID (string).
	var groupID string
	groupID, offset = readString(body, offset)
	var memberID string
	memberID, offset = readString(body, offset)
	_ = offset // consumed

	var errorCode int16
	if err := s.consumer.LeaveGroup(groupID, memberID); err != nil {
		errorCode = 25 // UNKNOWN_MEMBER_ID
	}

	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(errorCode))

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleListOffsets(req *KafkaRequest) (*KafkaResponse, error) {
	body := req.Body
	offset := 0

	// Decode: ReplicaID (int32), topic count, then per topic:
	// name, partition count, then per partition: partition(int32) + timestamp(int64) + max_offsets(int32).
	if offset+4 > len(body) {
		return &KafkaResponse{CorrelationID: req.CorrelationID, Body: make([]byte, 4)}, nil
	}
	offset += 4 // skip ReplicaID

	type partResult struct {
		partition int32
		timestamp int64
		resOffset int64
		errorCode int16
	}
	type topicResult struct {
		name       string
		partitions []partResult
	}
	var results []topicResult

	if offset+4 > len(body) {
		return &KafkaResponse{CorrelationID: req.CorrelationID, Body: make([]byte, 4)}, nil
	}
	topicCount := int(readInt32(body, offset))
	offset += 4

	for i := 0; i < topicCount && offset < len(body); i++ {
		var topicName string
		topicName, offset = readString(body, offset)

		tr := topicResult{name: topicName}

		if offset+4 > len(body) {
			break
		}
		partCount := int(readInt32(body, offset))
		offset += 4

		for j := 0; j < partCount && offset < len(body); j++ {
			if offset+16 > len(body) { // partition(4) + timestamp(8) + max_offsets(4)
				break
			}
			partition := readInt32(body, offset)
			offset += 4
			timestamp := readInt64(body, offset)
			offset += 8
			offset += 4 // skip max_offsets

			pr := partResult{partition: partition, timestamp: timestamp}

			topic := s.broker.GetTopic(topicName)
			if topic == nil {
				pr.errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
			} else {
				p := topic.GetPartition(partition)
				if p == nil {
					pr.errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
				} else {
					switch timestamp {
					case -1: // Latest
						pr.resOffset = p.HighWatermark()
						pr.timestamp = time.Now().UnixMilli()
					case -2: // Earliest
						pr.resOffset = 0
						pr.timestamp = time.Now().UnixMilli()
					default:
						// For a specific timestamp, return the high watermark as approximation.
						pr.resOffset = p.HighWatermark()
						pr.timestamp = timestamp
					}
				}
			}
			tr.partitions = append(tr.partitions, pr)
		}
		results = append(results, tr)
	}

	// Encode response.
	buf := make([]byte, 0, 4+len(results)*64)
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(results)))
	buf = append(buf, tmp...)

	for _, tr := range results {
		// Topic name.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(tr.name)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(tr.name)...)

		// Partition count.
		tmp = make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(tr.partitions)))
		buf = append(buf, tmp...)

		for _, pr := range tr.partitions {
			// Partition ID.
			tmp = make([]byte, 4)
			binary.BigEndian.PutUint32(tmp, uint32(pr.partition))
			buf = append(buf, tmp...)

			// Error code.
			tmp = make([]byte, 2)
			binary.BigEndian.PutUint16(tmp, uint16(pr.errorCode))
			buf = append(buf, tmp...)

			// Timestamp.
			tmp = make([]byte, 8)
			binary.BigEndian.PutUint64(tmp, uint64(pr.timestamp))
			buf = append(buf, tmp...)

			// Offset.
			tmp = make([]byte, 8)
			binary.BigEndian.PutUint64(tmp, uint64(pr.resOffset))
			buf = append(buf, tmp...)
		}
	}

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleCreateTopics(req *KafkaRequest) (*KafkaResponse, error) {
	body := req.Body
	offset := 0

	// Decode: topic count, then per topic: name + partitions(int32) +
	// replication_factor(int16) + assignments count + configs count.
	type topicCreateResult struct {
		name      string
		errorCode int16
	}
	var results []topicCreateResult

	if offset+4 > len(body) {
		return &KafkaResponse{CorrelationID: req.CorrelationID, Body: make([]byte, 4)}, nil
	}
	topicCount := int(readInt32(body, offset))
	offset += 4

	for i := 0; i < topicCount && offset < len(body); i++ {
		var topicName string
		topicName, offset = readString(body, offset)

		if offset+6 > len(body) { // partitions(4) + replication_factor(2)
			break
		}
		partitions := readInt32(body, offset)
		offset += 4
		offset += 2 // skip replication_factor

		// Skip replica assignments array.
		if offset+4 <= len(body) {
			assignmentCount := int(readInt32(body, offset))
			offset += 4
			for j := 0; j < assignmentCount && offset < len(body); j++ {
				offset += 4 // partition id
				if offset+4 <= len(body) {
					replicaCount := int(readInt32(body, offset))
					offset += 4
					offset += replicaCount * 4 // replica broker ids
				}
			}
		}

		// Skip configs array.
		if offset+4 <= len(body) {
			configCount := int(readInt32(body, offset))
			offset += 4
			for j := 0; j < configCount && offset < len(body); j++ {
				_, offset = readString(body, offset) // config key
				_, offset = readString(body, offset) // config value
			}
		}

		var errorCode int16
		if err := s.broker.CreateTopic(topicName, int(partitions)); err != nil {
			errorCode = 36 // TOPIC_ALREADY_EXISTS
		}

		results = append(results, topicCreateResult{name: topicName, errorCode: errorCode})
	}

	// Encode response: topic count, then per topic: name + error_code(int16).
	buf := make([]byte, 0, 4+len(results)*32)
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(results)))
	buf = append(buf, tmp...)

	for _, r := range results {
		// Topic name.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(r.name)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(r.name)...)

		// Error code.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(r.errorCode))
		buf = append(buf, tmp...)
	}

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleDeleteTopics(req *KafkaRequest) (*KafkaResponse, error) {
	body := req.Body
	offset := 0

	// Decode: topic count, then per topic: name; then timeout_ms(int32).
	type topicDeleteResult struct {
		name      string
		errorCode int16
	}
	var results []topicDeleteResult

	if offset+4 > len(body) {
		return &KafkaResponse{CorrelationID: req.CorrelationID, Body: make([]byte, 4)}, nil
	}
	topicCount := int(readInt32(body, offset))
	offset += 4

	for i := 0; i < topicCount && offset < len(body); i++ {
		var topicName string
		topicName, offset = readString(body, offset)

		var errorCode int16
		if err := s.broker.DeleteTopic(topicName); err != nil {
			errorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION
		}

		results = append(results, topicDeleteResult{name: topicName, errorCode: errorCode})
	}

	// Encode response: topic count, then per topic: name + error_code(int16).
	buf := make([]byte, 0, 4+len(results)*32)
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(results)))
	buf = append(buf, tmp...)

	for _, r := range results {
		// Topic name.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(len(r.name)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(r.name)...)

		// Error code.
		tmp = make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(r.errorCode))
		buf = append(buf, tmp...)
	}

	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          buf,
	}, nil
}

func (s *Server) handleAPIVersions(req *KafkaRequest) (*KafkaResponse, error) {
	return &KafkaResponse{
		CorrelationID: req.CorrelationID,
		Body:          encodeAPIVersionsResponse(),
	}, nil
}

// readKafkaRequest reads a Kafka request from the connection.
// Kafka request wire format:
//
//	Size (4 bytes, int32) | APIKey (2) | APIVersion (2) | CorrelationID (4) | ClientID (nullable string: 2-byte len + bytes) | Body...
func readKafkaRequest(r io.Reader) (*KafkaRequest, error) {
	// Read message size (4 bytes, big-endian).
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	if size < 8 || size > 100*1024*1024 { // min: APIKey(2)+APIVersion(2)+CorrelationID(4) = 8, max 100MB
		return nil, fmt.Errorf("invalid request size: %d", size)
	}

	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	apiKey := int16(binary.BigEndian.Uint16(body[0:2]))
	apiVersion := int16(binary.BigEndian.Uint16(body[2:4]))
	correlationID := int32(binary.BigEndian.Uint32(body[4:8]))

	// Parse ClientID (nullable string: 2-byte length prefix).
	offset := 8
	var clientID string
	if offset+2 <= len(body) {
		clientIDLen := int16(binary.BigEndian.Uint16(body[offset:]))
		offset += 2
		if clientIDLen > 0 && offset+int(clientIDLen) <= len(body) {
			clientID = string(body[offset : offset+int(clientIDLen)])
			offset += int(clientIDLen)
		}
	}

	return &KafkaRequest{
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
		Body:          body[offset:],
	}, nil
}

// writeKafkaResponse writes a Kafka response to the connection.
func writeKafkaResponse(w io.Writer, resp *KafkaResponse) error {
	// Response: Size (4) + CorrelationId (4) + Body.
	size := int32(4 + len(resp.Body))
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, resp.CorrelationID); err != nil {
		return err
	}
	if _, err := w.Write(resp.Body); err != nil {
		return err
	}
	return nil
}
