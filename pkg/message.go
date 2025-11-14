package pkg

import (
	"fmt"
	"strings"
	"unicode/utf16"
)

// TDSMessage TDS消息接口
type TDSMessage interface {
	IsComplete() bool
	HasIgnoreBitSet() bool
	AssemblePayload() []byte
	AddPacket(packet *TDSPacket)
	GetPackets() []*TDSPacket
	String() string
}

// BaseTDSMessage TDS消息基类
type BaseTDSMessage struct {
	Packets []*TDSPacket
}

// NewBaseTDSMessage 创建新的BaseTDSMessage
func NewBaseTDSMessage() *BaseTDSMessage {
	return &BaseTDSMessage{
		Packets: make([]*TDSPacket, 0),
	}
}

// NewBaseTDSMessageWithPacket 从第一个数据包创建新的BaseTDSMessage
func NewBaseTDSMessageWithPacket(firstPacket *TDSPacket) *BaseTDSMessage {
	return &BaseTDSMessage{
		Packets: []*TDSPacket{firstPacket},
	}
}

// IsComplete 检查消息是否完整
func (m *BaseTDSMessage) IsComplete() bool {
	if len(m.Packets) == 0 {
		return false
	}
	lastPacket := m.Packets[len(m.Packets)-1]
	return (lastPacket.Header.StatusBitMask() & END_OF_MESSAGE) == END_OF_MESSAGE
}

// HasIgnoreBitSet 检查是否设置了忽略位
func (m *BaseTDSMessage) HasIgnoreBitSet() bool {
	if len(m.Packets) == 0 {
		return false
	}
	lastPacket := m.Packets[len(m.Packets)-1]
	return (lastPacket.Header.StatusBitMask() & IGNORE_EVENT) == IGNORE_EVENT
}

// AssemblePayload 组装有效载荷
func (m *BaseTDSMessage) AssemblePayload() []byte {
	var totalSize int
	for _, packet := range m.Packets {
		totalSize += len(packet.Payload)
	}

	payload := make([]byte, totalSize)
	currentPosition := 0

	for _, packet := range m.Packets {
		copy(payload[currentPosition:], packet.Payload)
		currentPosition += len(packet.Payload)
	}

	return payload
}

// AddPacket 添加数据包
func (m *BaseTDSMessage) AddPacket(packet *TDSPacket) {
	m.Packets = append(m.Packets, packet)
}

// GetPackets 获取所有数据包
func (m *BaseTDSMessage) GetPackets() []*TDSPacket {
	return m.Packets
}

// DefaultTDSMessage 默认TDS消息实现
type DefaultTDSMessage struct {
	*BaseTDSMessage
}

// NewDefaultTDSMessage 创建新的DefaultTDSMessage
func NewDefaultTDSMessage() *DefaultTDSMessage {
	return &DefaultTDSMessage{
		BaseTDSMessage: NewBaseTDSMessage(),
	}
}

// NewDefaultTDSMessageWithPacket 从第一个数据包创建新的DefaultTDSMessage
func NewDefaultTDSMessageWithPacket(firstPacket *TDSPacket) *DefaultTDSMessage {
	return &DefaultTDSMessage{
		BaseTDSMessage: NewBaseTDSMessageWithPacket(firstPacket),
	}
}

func (m *DefaultTDSMessage) String() string {
	if m.IsComplete() {
		sb := strings.Builder{}
		sb.WriteString("DefaultTDSMessage")
		sb.WriteString(fmt.Sprintf("[#Packets=%d;IsComplete=%v;HasIgnoreBitSet=%v;TotalPayloadSize=%d",
			len(m.Packets), m.IsComplete(), m.HasIgnoreBitSet(), len(m.AssemblePayload())))

		for i, packet := range m.Packets {
			sb.WriteString(fmt.Sprintf("\n\t[P%d[%s]]", i, packet))
		}

		sb.WriteString("]")
		return sb.String()
	}
	return "DefaultTDSMessage{Incomplete message}"
}

// SQLBatchMessage SQL批处理消息
type SQLBatchMessage struct {
	*BaseTDSMessage
}

// NewSQLBatchMessage 创建新的SQLBatchMessage
func NewSQLBatchMessage() *SQLBatchMessage {
	return &SQLBatchMessage{
		BaseTDSMessage: NewBaseTDSMessage(),
	}
}

// NewSQLBatchMessageWithPacket 从第一个数据包创建新的SQLBatchMessage
func NewSQLBatchMessageWithPacket(firstPacket *TDSPacket) *SQLBatchMessage {
	return &SQLBatchMessage{
		BaseTDSMessage: NewBaseTDSMessageWithPacket(firstPacket),
	}
}

// GetBatchText 获取批处理文本
func (m *SQLBatchMessage) GetBatchText() string {
	payload := m.AssemblePayload()
	allHeader := NewAllHeader(payload)
	headerLength := int(allHeader.Length())

	if len(payload) > headerLength {
		// SQL Server使用UTF-16编码
		utf16Bytes := payload[headerLength:]
		// 转换UTF-16字节为rune数组
		utf16Runes := make([]uint16, len(utf16Bytes)/2)
		for i := 0; i < len(utf16Runes); i++ {
			utf16Runes[i] = uint16(utf16Bytes[i*2]) + uint16(utf16Bytes[i*2+1])<<8
		}
		// 转换为UTF-8字符串
		return string(utf16.Decode(utf16Runes))
	}
	return ""
}

func (m *SQLBatchMessage) String() string {
	if m.IsComplete() {
		sb := strings.Builder{}
		sb.WriteString("SQLBatchMessage")
		sb.WriteString(fmt.Sprintf("[#Packets=%d;IsComplete=%v;HasIgnoreBitSet=%v;TotalPayloadSize=%d",
			len(m.Packets), m.IsComplete(), m.HasIgnoreBitSet(), len(m.AssemblePayload())))

		for i, packet := range m.Packets {
			sb.WriteString(fmt.Sprintf("\n\t[P%d[%s]]", i, packet))
		}

		sb.WriteString("]")
		return sb.String()
	}
	return "SQLBatchMessage{Incomplete message}"
}

// RPCRequestMessage RPC请求消息
type RPCRequestMessage struct {
	*BaseTDSMessage
}

// NewRPCRequestMessage 创建新的RPCRequestMessage
func NewRPCRequestMessage() *RPCRequestMessage {
	return &RPCRequestMessage{
		BaseTDSMessage: NewBaseTDSMessage(),
	}
}

// NewRPCRequestMessageWithPacket 从第一个数据包创建新的RPCRequestMessage
func NewRPCRequestMessageWithPacket(firstPacket *TDSPacket) *RPCRequestMessage {
	return &RPCRequestMessage{
		BaseTDSMessage: NewBaseTDSMessageWithPacket(firstPacket),
	}
}

func (m *RPCRequestMessage) String() string {
	if m.IsComplete() {
		sb := strings.Builder{}
		sb.WriteString("RPCRequestMessage")
		sb.WriteString(fmt.Sprintf("[#Packets=%d;IsComplete=%v;HasIgnoreBitSet=%v;TotalPayloadSize=%d",
			len(m.Packets), m.IsComplete(), m.HasIgnoreBitSet(), len(m.AssemblePayload())))

		for i, packet := range m.Packets {
			sb.WriteString(fmt.Sprintf("\n\t[P%d[%s]]", i, packet))
		}

		sb.WriteString("]")
		return sb.String()
	}
	return "RPCRequestMessage{Incomplete message}"
}

// AttentionMessage 注意信号消息
type AttentionMessage struct {
	*BaseTDSMessage
}

// NewAttentionMessage 创建新的AttentionMessage
func NewAttentionMessage() *AttentionMessage {
	return &AttentionMessage{
		BaseTDSMessage: NewBaseTDSMessage(),
	}
}

// NewAttentionMessageWithPacket 从第一个数据包创建新的AttentionMessage
func NewAttentionMessageWithPacket(firstPacket *TDSPacket) *AttentionMessage {
	return &AttentionMessage{
		BaseTDSMessage: NewBaseTDSMessageWithPacket(firstPacket),
	}
}

func (m *AttentionMessage) String() string {
	if m.IsComplete() {
		sb := strings.Builder{}
		sb.WriteString("AttentionMessage")
		sb.WriteString(fmt.Sprintf("[#Packets=%d;IsComplete=%v;HasIgnoreBitSet=%v;TotalPayloadSize=%d",
			len(m.Packets), m.IsComplete(), m.HasIgnoreBitSet(), len(m.AssemblePayload())))

		for i, packet := range m.Packets {
			sb.WriteString(fmt.Sprintf("\n\t[P%d[%s]]", i, packet))
		}

		sb.WriteString("]")
		return sb.String()
	}
	return "AttentionMessage{Incomplete message}"
}

// CreateTDSMessageFromFirstPacket 从第一个数据包创建对应的TDS消息类型
func CreateTDSMessageFromFirstPacket(firstPacket *TDSPacket) TDSMessage {
	switch firstPacket.Header.Type() {
	case SQLBatch:
		return NewSQLBatchMessageWithPacket(firstPacket)
	case AttentionSignal:
		return NewAttentionMessageWithPacket(firstPacket)
	case RPC:
		return NewRPCRequestMessageWithPacket(firstPacket)
	default:
		return NewDefaultTDSMessageWithPacket(firstPacket)
	}
}