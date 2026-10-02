// Marabunta - Licensed under the MIT License.
// Package terminal implements the TN3270 TCP server for CICS terminal access.
package terminal

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/marabunta/marabunta-cics/internal/transaction"
)

// Server is a TN3270 TCP listener for 3270 terminal emulators.
type Server struct {
	addr     string
	listener net.Listener
	executor *transaction.Executor
	regionID string
	closed   atomic.Bool
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[uint64]*Session
	nextID   uint64
}

// Session represents a single TN3270 terminal session.
type Session struct {
	id       uint64
	conn     net.Conn
	executor *transaction.Executor
	regionID string
	termID   string // terminal ID (4 chars)
	screen   *Screen
}

// Screen represents a 3270 terminal screen buffer.
type Screen struct {
	Rows     int
	Cols     int
	Buffer   []byte
	Modified bool
}

// NewServer creates a new TN3270 server.
func NewServer(addr string, executor *transaction.Executor, regionID string) *Server {
	return &Server{
		addr:     addr,
		executor: executor,
		regionID: regionID,
		conns:    make(map[uint64]*Session),
	}
}

// ListenAndServe starts the TN3270 listener.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
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
			log.Printf("tn3270: accept error: %v", err)
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
	s.mu.Lock()
	for _, sess := range s.conns {
		sess.conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()

	id := atomic.AddUint64(&s.nextID, 1)
	sess := &Session{
		id:       id,
		conn:     conn,
		executor: s.executor,
		regionID: s.regionID,
		termID:   generateTermID(id),
		screen: &Screen{
			Rows:   24,
			Cols:   80,
			Buffer: make([]byte, 24*80),
		},
	}

	s.mu.Lock()
	s.conns[id] = sess
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.conns, id)
		s.mu.Unlock()
		conn.Close()
	}()

	log.Printf("tn3270: session %d connected from %s", id, conn.RemoteAddr())

	// Perform TN3270 telnet negotiation.
	if err := sess.negotiate(); err != nil {
		log.Printf("tn3270: session %d negotiate error: %v", id, err)
		return
	}

	// Send welcome screen.
	if err := sess.sendWelcome(); err != nil {
		log.Printf("tn3270: session %d welcome error: %v", id, err)
		return
	}

	// Main session loop.
	if err := sess.serve(); err != nil {
		if err != io.EOF && !s.closed.Load() {
			log.Printf("tn3270: session %d error: %v", id, err)
		}
	}
}

// negotiate performs TN3270 telnet negotiation.
func (s *Session) negotiate() error {
	// TN3270E negotiation sequence.
	// IAC DO TN3270E.
	negotiation := []byte{
		0xFF, 0xFD, 0x28, // IAC DO TN3270E
		0xFF, 0xFD, 0x19, // IAC DO EOR
		0xFF, 0xFB, 0x19, // IAC WILL EOR
	}
	if _, err := s.conn.Write(negotiation); err != nil {
		return err
	}

	// Read and discard client negotiation response.
	// The client may send WILL/WONT/DO/DONT responses of varying lengths;
	// we consume whatever is available. Only a hard I/O error is fatal.
	buf := make([]byte, 256)
	_, err := s.conn.Read(buf)
	if err != nil && err != io.EOF {
		return fmt.Errorf("telnet negotiation read: %w", err)
	}

	return nil
}

// sendWelcome sends the CICS welcome screen.
func (s *Session) sendWelcome() error {
	screen := make([]byte, 0, 256)

	// Write command: Erase/Write.
	screen = append(screen, 0xF5) // Erase/Write
	screen = append(screen, 0xC3) // WCC: reset MDT

	// Position to row 1, col 20.
	screen = append(screen, 0x11)                    // SBA
	screen = append(screen, bufferAddress(0, 19)...) // row 1, col 20

	// Write title.
	title := ebcdicEncode("MARABUNTA CICS REGION " + s.regionID)
	screen = append(screen, title...)

	// Position to row 10, col 25.
	screen = append(screen, 0x11)
	screen = append(screen, bufferAddress(9, 24)...)
	screen = append(screen, ebcdicEncode("ENTER TRANSACTION:")...)

	// Start field for input.
	screen = append(screen, 0x1D) // SF
	screen = append(screen, 0x40) // unprotected, MDT off

	// Send with EOR.
	frame := append(screen, 0xFF, 0xEF) // IAC EOR
	_, err := s.conn.Write(frame)
	return err
}

// serve is the main session loop processing 3270 data streams.
func (s *Session) serve() error {
	buf := make([]byte, 4096)
	for {
		n, err := s.conn.Read(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			continue
		}

		data := buf[:n]

		// Strip IAC EOR suffix.
		if n >= 2 && data[n-2] == 0xFF && data[n-1] == 0xEF {
			data = data[:n-2]
		}

		if len(data) == 0 {
			continue
		}

		// Parse 3270 data stream.
		aid, fields := parse3270Input(data)
		if err := s.processInput(aid, fields); err != nil {
			return err
		}
	}
}

// processInput handles parsed 3270 input from the terminal.
func (s *Session) processInput(aid byte, fields map[int]string) error {
	// AID bytes: 0x7D = Enter, 0xF1-F9 = PF1-PF9, etc.
	switch aid {
	case 0x7D: // Enter
		// Get the transaction input.
		input := ""
		for _, v := range fields {
			input = v
			break
		}
		if input != "" {
			return s.executeTransaction(input)
		}
		return nil
	case 0xF3: // PF3 = exit
		if err := s.sendGoodbye(); err != nil {
			return err
		}
		return s.conn.Close()
	default:
		// Other AID keys — just redisplay.
		return s.sendWelcome()
	}
}

// executeTransaction runs a CICS transaction.
func (s *Session) executeTransaction(input string) error {
	// First 4 characters are the transaction ID.
	tranID := input
	data := ""
	if len(input) > 4 {
		tranID = input[:4]
		data = input[4:]
	}

	result, err := s.executor.Execute(tranID, data, s.termID)
	if err != nil {
		return s.sendScreen("TRANSACTION ERROR: " + err.Error())
	}
	return s.sendScreen(result)
}

// sendScreen sends a text string to the terminal.
func (s *Session) sendScreen(text string) error {
	screen := make([]byte, 0, len(text)+20)
	screen = append(screen, 0xF5) // Erase/Write
	screen = append(screen, 0xC3) // WCC

	screen = append(screen, 0x11)
	screen = append(screen, bufferAddress(0, 0)...)
	screen = append(screen, ebcdicEncode(text)...)

	frame := append(screen, 0xFF, 0xEF)
	_, err := s.conn.Write(frame)
	return err
}

// sendGoodbye sends a goodbye message.
func (s *Session) sendGoodbye() error {
	return s.sendScreen("SESSION ENDED. GOODBYE.")
}

// generateTermID generates a 4-character terminal ID from a session number.
func generateTermID(id uint64) string {
	chars := "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, 4)
	for i := 3; i >= 0; i-- {
		result[i] = chars[id%36]
		id /= 36
	}
	return string(result)
}

// bufferAddress encodes a row,col position as a 2-byte 3270 buffer address.
func bufferAddress(row, col int) []byte {
	addr := row*80 + col
	// 12-bit encoding.
	b1 := byte(((addr >> 6) & 0x3F) | 0x40)
	b2 := byte((addr & 0x3F) | 0x40)
	return []byte{b1, b2}
}

// ebcdicEncode converts ASCII text to EBCDIC for 3270 display.
// Simplified: maps printable ASCII to EBCDIC equivalents.
// Only processes single-byte characters; multi-byte runes are replaced with EBCDIC space.
func ebcdicEncode(s string) []byte {
	data := []byte(s)
	result := make([]byte, len(data))
	for i, b := range data {
		result[i] = asciiToEBCDIC(b)
	}
	return result
}

// asciiToEBCDIC converts a single ASCII byte to EBCDIC.
func asciiToEBCDIC(b byte) byte {
	// Simplified ASCII to EBCDIC conversion table.
	switch {
	case b >= 'A' && b <= 'I':
		return b - 'A' + 0xC1
	case b >= 'J' && b <= 'R':
		return b - 'J' + 0xD1
	case b >= 'S' && b <= 'Z':
		return b - 'S' + 0xE2
	case b >= 'a' && b <= 'i':
		return b - 'a' + 0x81
	case b >= 'j' && b <= 'r':
		return b - 'j' + 0x91
	case b >= 's' && b <= 'z':
		return b - 's' + 0xA2
	case b >= '0' && b <= '9':
		return b - '0' + 0xF0
	case b == ' ':
		return 0x40
	case b == '.':
		return 0x4B
	case b == ',':
		return 0x6B
	case b == ':':
		return 0x7A
	case b == '-':
		return 0x60
	case b == '/':
		return 0x61
	case b == '(':
		return 0x4D
	case b == ')':
		return 0x5D
	case b == '=':
		return 0x7E
	case b == '\'':
		return 0x7D
	default:
		return 0x40 // space for unmapped chars
	}
}

// parse3270Input parses a 3270 inbound data stream.
// Returns the AID byte and a map of field positions to values.
func parse3270Input(data []byte) (byte, map[int]string) {
	fields := make(map[int]string)
	if len(data) == 0 {
		return 0, fields
	}

	aid := data[0]
	if len(data) < 3 {
		return aid, fields
	}

	// Skip cursor address (2 bytes after AID).
	pos := 3

	// Parse field data.
	currentField := 0
	var fieldData []byte

	for pos < len(data) {
		if data[pos] == 0x11 { // SBA (Set Buffer Address)
			if len(fieldData) > 0 {
				fields[currentField] = ebcdicDecode(fieldData)
				fieldData = nil
			}
			if pos+2 < len(data) {
				b1 := data[pos+1]
				b2 := data[pos+2]
				currentField = int((b1&0x3F))<<6 | int(b2&0x3F)
				pos += 3
			} else {
				break
			}
		} else {
			fieldData = append(fieldData, data[pos])
			pos++
		}
	}

	// Save last field.
	if len(fieldData) > 0 {
		fields[currentField] = ebcdicDecode(fieldData)
	}

	return aid, fields
}

// ebcdicDecode converts EBCDIC bytes to ASCII string.
func ebcdicDecode(data []byte) string {
	result := make([]byte, len(data))
	for i, b := range data {
		result[i] = ebcdicToASCII(b)
	}
	return string(result)
}

// ebcdicToASCII converts a single EBCDIC byte to ASCII.
func ebcdicToASCII(b byte) byte {
	switch {
	case b >= 0xC1 && b <= 0xC9:
		return b - 0xC1 + 'A'
	case b >= 0xD1 && b <= 0xD9:
		return b - 0xD1 + 'J'
	case b >= 0xE2 && b <= 0xE9:
		return b - 0xE2 + 'S'
	case b >= 0x81 && b <= 0x89:
		return b - 0x81 + 'a'
	case b >= 0x91 && b <= 0x99:
		return b - 0x91 + 'j'
	case b >= 0xA2 && b <= 0xA9:
		return b - 0xA2 + 's'
	case b >= 0xF0 && b <= 0xF9:
		return b - 0xF0 + '0'
	case b == 0x40:
		return ' '
	case b == 0x4B:
		return '.'
	case b == 0x6B:
		return ','
	case b == 0x7A:
		return ':'
	case b == 0x60:
		return '-'
	case b == 0x61:
		return '/'
	case b == 0x4D:
		return '('
	case b == 0x5D:
		return ')'
	case b == 0x7E:
		return '='
	case b == 0x7D:
		return '\''
	default:
		return ' '
	}
}
