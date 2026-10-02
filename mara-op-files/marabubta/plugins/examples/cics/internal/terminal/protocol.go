// Marabunta - Licensed under the MIT License.
package terminal

// 3270 data stream constants.
const (
	// Commands (host to terminal).
	CmdWrite          byte = 0xF1 // Write
	CmdEraseWrite     byte = 0xF5 // Erase/Write
	CmdEraseWriteAlt  byte = 0x7E // Erase/Write Alternate
	CmdReadBuffer     byte = 0xF2 // Read Buffer
	CmdReadModified   byte = 0xF6 // Read Modified
	CmdReadModifiedAll byte = 0x6E // Read Modified All
	CmdEraseAllUnprot byte = 0x6F // Erase All Unprotected
	CmdWSF            byte = 0xF3 // Write Structured Field

	// Orders.
	OrderSBA byte = 0x11 // Set Buffer Address
	OrderSF  byte = 0x1D // Start Field
	OrderSFE byte = 0x29 // Start Field Extended
	OrderSA  byte = 0x28 // Set Attribute
	OrderMF  byte = 0x2C // Modify Field
	OrderIC  byte = 0x13 // Insert Cursor
	OrderPT  byte = 0x05 // Program Tab
	OrderRA  byte = 0x3C // Repeat to Address
	OrderEUA byte = 0x12 // Erase Unprotected to Address
	OrderGE  byte = 0x08 // Graphic Escape

	// AID (Attention Identifier) bytes.
	AIDEnter byte = 0x7D
	AIDPF1   byte = 0xF1
	AIDPF2   byte = 0xF2
	AIDPF3   byte = 0xF3
	AIDPF4   byte = 0xF4
	AIDPF5   byte = 0xF5
	AIDPF6   byte = 0xF6
	AIDPF7   byte = 0xF7
	AIDPF8   byte = 0xF8
	AIDPF9   byte = 0xF9
	AIDPF10  byte = 0x7A
	AIDPF11  byte = 0x7B
	AIDPF12  byte = 0x7C
	AIDPA1   byte = 0x6C
	AIDPA2   byte = 0x6E
	AIDPA3   byte = 0x6B
	AIDClear byte = 0x6D

	// Write Control Character (WCC) bits.
	WCCReset     byte = 0x40 // Reset MDT
	WCCAlarm     byte = 0x04 // Sound alarm
	WCCUnlock    byte = 0x02 // Unlock keyboard
	WCCResetMDT  byte = 0x01 // Reset MDT in each field

	// Field attribute bits.
	FAProtected    byte = 0x20
	FANumeric      byte = 0x10
	FAHighIntensity byte = 0x08
	FAModified     byte = 0x01
	FAHidden       byte = 0x0C // non-display

	// Telnet constants.
	TelnetIAC  byte = 0xFF
	TelnetEOR  byte = 0xEF
	TelnetSE   byte = 0xF0
	TelnetSB   byte = 0xFA
	TelnetWILL byte = 0xFB
	TelnetWONT byte = 0xFC
	TelnetDO   byte = 0xFD
	TelnetDONT byte = 0xFE
	TelnetTN3270E byte = 0x28
)

// DataStream3270 builds a 3270 data stream for the terminal.
type DataStream3270 struct {
	buf []byte
}

// NewDataStream creates a new 3270 data stream builder.
func NewDataStream() *DataStream3270 {
	return &DataStream3270{}
}

// EraseWrite starts the stream with an Erase/Write command.
func (ds *DataStream3270) EraseWrite(wcc byte) *DataStream3270 {
	ds.buf = append(ds.buf, CmdEraseWrite, wcc)
	return ds
}

// Write starts the stream with a Write command.
func (ds *DataStream3270) Write(wcc byte) *DataStream3270 {
	ds.buf = append(ds.buf, CmdWrite, wcc)
	return ds
}

// SBA sets the buffer address to the given row and column.
func (ds *DataStream3270) SBA(row, col int) *DataStream3270 {
	ds.buf = append(ds.buf, OrderSBA)
	ds.buf = append(ds.buf, bufferAddress(row, col)...)
	return ds
}

// SF starts a new field with the given attribute.
func (ds *DataStream3270) SF(attr byte) *DataStream3270 {
	ds.buf = append(ds.buf, OrderSF, attr)
	return ds
}

// IC inserts the cursor at the current position.
func (ds *DataStream3270) IC() *DataStream3270 {
	ds.buf = append(ds.buf, OrderIC)
	return ds
}

// Text writes EBCDIC-encoded text.
func (ds *DataStream3270) Text(s string) *DataStream3270 {
	ds.buf = append(ds.buf, ebcdicEncode(s)...)
	return ds
}

// RA repeats a character to the given address.
func (ds *DataStream3270) RA(row, col int, ch byte) *DataStream3270 {
	ds.buf = append(ds.buf, OrderRA)
	ds.buf = append(ds.buf, bufferAddress(row, col)...)
	ds.buf = append(ds.buf, ch)
	return ds
}

// Bytes returns the raw data stream bytes (without telnet framing).
func (ds *DataStream3270) Bytes() []byte {
	return ds.buf
}

// Frame wraps the data stream with IAC EOR for telnet transmission.
func (ds *DataStream3270) Frame() []byte {
	framed := make([]byte, len(ds.buf)+2)
	copy(framed, ds.buf)
	framed[len(ds.buf)] = TelnetIAC
	framed[len(ds.buf)+1] = TelnetEOR
	return framed
}
