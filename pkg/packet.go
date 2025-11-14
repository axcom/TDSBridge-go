package pkg

import "fmt"

// TDSPacket TDS数据包结构体
type TDSPacket struct {
	Header  *TDSHeader
	Payload []byte
}

// NewTDSPacketFromBuffer 从完整缓冲区创建新的TDSPacket
func NewTDSPacketFromBuffer(buffer []byte) *TDSPacket {
	header := NewTDSHeader(buffer)
	payloadSize := header.PayloadSize()
	payload := make([]byte, payloadSize)
	if len(buffer) >= HEADER_SIZE+payloadSize {
		copy(payload, buffer[HEADER_SIZE:HEADER_SIZE+payloadSize])
	}
	return &TDSPacket{
		Header:  header,
		Payload: payload,
	}
}

// NewTDSPacket 从头部和负载创建新的TDSPacket
func NewTDSPacket(header []byte, payload []byte, payloadSize int) *TDSPacket {
	tHeader := NewTDSHeader(header)
	tPayload := make([]byte, payloadSize)
	if len(payload) >= payloadSize {
		copy(tPayload, payload[:payloadSize])
	}
	return &TDSPacket{
		Header:  tHeader,
		Payload: tPayload,
	}
}

func (p *TDSPacket) String() string {
	return fmt.Sprintf("TDSPacket[Header=%s]", p.Header)
}