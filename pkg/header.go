package pkg

import "fmt"

// HeaderType TDS头部类型枚举
type HeaderType int

const (
	SQLBatch              HeaderType = 1
	PreTD7Login           HeaderType = 2
	RPC                   HeaderType = 3
	TabularResult         HeaderType = 4
	AttentionSignal       HeaderType = 6
	BulkLoadData          HeaderType = 7
	TransactionManagerRequest HeaderType = 14
	TDS7Login             HeaderType = 16
	SSPIMessage           HeaderType = 17
	PreLoginMessage       HeaderType = 18
	UnknownHeader         HeaderType = 0xFF
)

func (ht HeaderType) String() string {
	switch ht {
	case SQLBatch:
		return "SQLBatch"
	case PreTD7Login:
		return "PreTD7Login"
	case RPC:
		return "RPC"
	case TabularResult:
		return "TabularResult"
	case AttentionSignal:
		return "AttentionSignal"
	case BulkLoadData:
		return "BulkLoadData"
	case TransactionManagerRequest:
		return "TransactionManagerRequest"
	case TDS7Login:
		return "TDS7Login"
	case SSPIMessage:
		return "SSPIMessage"
	case PreLoginMessage:
		return "PreLoginMessage"
	default:
		return "Unknown"
	}
}

// StatusBitMask 状态位掩码常量
const (
	NORMAL                    = 0x00
	END_OF_MESSAGE            = 0x01
	IGNORE_EVENT              = 0x02
	MULTI_PART_MESSAGE        = 0x04
	RESET_CONNECTION          = 0x08
	RESET_CONNECTION_SKIP_TRAN = 0x10
)

// TDSHeader TDS头部结构体
type TDSHeader struct {
	Buffer []byte
}

const HEADER_SIZE = 8

// NewTDSHeader 创建新的TDSHeader
func NewTDSHeader(buffer []byte) *TDSHeader {
	h := &TDSHeader{
		Buffer: make([]byte, HEADER_SIZE),
	}
	if len(buffer) >= HEADER_SIZE {
		copy(h.Buffer, buffer[:HEADER_SIZE])
	}
	return h
}

// Type 获取头部类型
func (h *TDSHeader) Type() HeaderType {
	return HeaderType(h.Buffer[0])
}

// StatusBitMask 获取状态位掩码
func (h *TDSHeader) StatusBitMask() byte {
	return h.Buffer[1]
}

// LengthIncludingHeader 获取包括头部的总长度
func (h *TDSHeader) LengthIncludingHeader() int {
	return int(h.Buffer[2])*0x100 + int(h.Buffer[3])
}

// PayloadSize 获取有效载荷大小
func (h *TDSHeader) PayloadSize() int {
	return h.LengthIncludingHeader() - HEADER_SIZE
}

// GetByte 获取指定索引的字节
func (h *TDSHeader) GetByte(idx int) byte {
	if idx >= 0 && idx < len(h.Buffer) {
		return h.Buffer[idx]
	}
	return 0
}

// SetByte 设置指定索引的字节
func (h *TDSHeader) SetByte(idx int, value byte) {
	if idx >= 0 && idx < len(h.Buffer) {
		h.Buffer[idx] = value
	}
}

func (h *TDSHeader) String() string {
	return fmt.Sprintf("TDSHeader[Type=%v;StatusBitMask=%v;LengthIncludingHeader=%v;PayloadSize=%v]",
		h.Type(), h.StatusBitMask(), h.LengthIncludingHeader(), h.PayloadSize())
}

// AllHeader 全部头部结构体
type AllHeader struct {
	Payload []byte
}

// NewAllHeader 创建新的AllHeader
func NewAllHeader(payload []byte) *AllHeader {
	return &AllHeader{
		Payload: payload,
	}
}

// Length 获取长度
func (ah *AllHeader) Length() uint32 {
	if len(ah.Payload) >= 4 {
		return uint32(ah.Payload[3])*0x01000000 +
			uint32(ah.Payload[2])*0x00010000 +
			uint32(ah.Payload[1])*0x00000100 +
			uint32(ah.Payload[0])*0x00000001
	}
	return 0
}