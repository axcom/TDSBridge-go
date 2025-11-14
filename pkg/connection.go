package pkg

import (
	"fmt"
	"net"
	"sync"
)

// ConnectionType 连接类型枚举
type ConnectionType int

const (
	ClientBridge ConnectionType = iota
	BridgeSQL
)

func (ct ConnectionType) String() string {
	switch ct {
	case ClientBridge:
		return "ClientBridge"
	case BridgeSQL:
		return "BridgeSQL"
	default:
		return "Unknown"
	}
}

// SocketCouple 套接字对结构体
type SocketCouple struct {
	ClientBridgeSocket net.Conn
	BridgeSQLSocket    net.Conn
}

func (sc *SocketCouple) String() string {
	if sc.ClientBridgeSocket == nil || sc.BridgeSQLSocket == nil {
		return fmt.Sprintf("SocketCouple[ClientBridgeSocket=%v, BridgeSQLSocket=%v]", sc.ClientBridgeSocket, sc.BridgeSQLSocket)
	}
	return fmt.Sprintf("SocketCouple[ClientBridgeSocket.RemoteEndPoint=%v, BridgeSQLSocket.RemoteEndPoint=%v]", 
		sc.ClientBridgeSocket.RemoteAddr(), sc.BridgeSQLSocket.RemoteAddr())
}

// 事件处理函数类型定义
type TDSMessageReceivedHandler func(*BridgedConnection, TDSMessage)
type TDSPacketReceivedHandler func(*BridgedConnection, *TDSPacket)
type ConnectionAcceptedHandler func(net.Conn)
type BridgeExceptionHandler func(*BridgedConnection, ConnectionType, error)
type ListeningThreadExceptionHandler func(net.Listener, error)
type ConnectionDisconnectedHandler func(*BridgedConnection, ConnectionType)

// BridgeAcceptor 桥接接收器结构体
type BridgeAcceptor struct {
	acceptPort        string
	sqlServerEndpoint string

	listener net.Listener
	enabled  bool
	mu       sync.Mutex

	// 事件处理函数
	tDSMessageReceivedHandler      TDSMessageReceivedHandler
	tDSPacketReceivedHandler       TDSPacketReceivedHandler
	connectionAcceptedHandler      ConnectionAcceptedHandler
	bridgeExceptionHandler         BridgeExceptionHandler
	listeningThreadExceptionHandler ListeningThreadExceptionHandler
	connectionDisconnectedHandler  ConnectionDisconnectedHandler
}

// NewBridgeAcceptor 创建新的BridgeAcceptor
func NewBridgeAcceptor(acceptPort, sqlServerEndpoint string) *BridgeAcceptor {
	return &BridgeAcceptor{
		acceptPort:        acceptPort,
		sqlServerEndpoint: sqlServerEndpoint,
		enabled:           false,
	}
}

// SetTDSMessageReceivedHandler 设置TDS消息接收处理函数
func (ba *BridgeAcceptor) SetTDSMessageReceivedHandler(handler TDSMessageReceivedHandler) {
	ba.tDSMessageReceivedHandler = handler
}

// SetTDSPacketReceivedHandler 设置TDS数据包接收处理函数
func (ba *BridgeAcceptor) SetTDSPacketReceivedHandler(handler TDSPacketReceivedHandler) {
	ba.tDSPacketReceivedHandler = handler
}

// SetConnectionAcceptedHandler 设置连接接受处理函数
func (ba *BridgeAcceptor) SetConnectionAcceptedHandler(handler ConnectionAcceptedHandler) {
	ba.connectionAcceptedHandler = handler
}

// SetConnectionDisconnectedHandler 设置连接断开处理函数
func (ba *BridgeAcceptor) SetConnectionDisconnectedHandler(handler ConnectionDisconnectedHandler) {
	ba.connectionDisconnectedHandler = handler
}

// SetBridgeExceptionHandler 设置桥接异常处理函数
func (ba *BridgeAcceptor) SetBridgeExceptionHandler(handler BridgeExceptionHandler) {
	ba.bridgeExceptionHandler = handler
}

// SetListeningThreadExceptionHandler 设置监听线程异常处理函数
func (ba *BridgeAcceptor) SetListeningThreadExceptionHandler(handler ListeningThreadExceptionHandler) {
	ba.listeningThreadExceptionHandler = handler
}

// Start 启动BridgeAcceptor
func (ba *BridgeAcceptor) Start() error {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	if ba.enabled {
		return nil // 已经在运行中
	}

	ba.enabled = true

	// 创建监听套接字
	listener, err := net.Listen("tcp", ":"+ba.acceptPort)
	if err != nil {
		ba.enabled = false
		return err
	}
	ba.listener = listener

	// 启动接受连接的goroutine
	go ba.acceptLoop()

	return nil
}

// Stop 停止BridgeAcceptor
func (ba *BridgeAcceptor) Stop() {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	if !ba.enabled {
		return
	}

	ba.enabled = false

	// 关闭监听器
	if ba.listener != nil {
		ba.listener.Close()
		ba.listener = nil
	}
}

// acceptLoop 接受连接的循环
func (ba *BridgeAcceptor) acceptLoop() {
	for ba.isEnabled() {
		// 接受客户端连接
		clientConn, err := ba.listener.Accept()
		if err != nil {
			if ba.isEnabled() { // 只有在启用状态下才报告错误
				ba.onListeningThreadException(ba.listener, err)
			}
			continue
		}

		// 处理新连接
		go ba.handleNewConnection(clientConn)
	}
}

// handleNewConnection 处理新的客户端连接
func (ba *BridgeAcceptor) handleNewConnection(clientConn net.Conn) {
	// 通知连接已接受
	ba.onConnectionAccepted(clientConn)

	// 连接到SQL Server
	sqlConn, err := net.Dial("tcp", ba.sqlServerEndpoint)
	if err != nil {
		clientConn.Close()
		// 这里可以触发异常事件
		return
	}

	// 创建SocketCouple
	socketCouple := &SocketCouple{
		ClientBridgeSocket: clientConn,
		BridgeSQLSocket:    sqlConn,
	}

	// 创建BridgedConnection
	bridgedConn := NewBridgedConnection(ba, socketCouple)

	// 启动桥接连接
	bridgedConn.Start()
}

// isEnabled 检查是否启用
func (ba *BridgeAcceptor) isEnabled() bool {
	ba.mu.Lock()
	defer ba.mu.Unlock()
	return ba.enabled
}

// onTDSMessageReceived 触发TDS消息接收事件
func (ba *BridgeAcceptor) onTDSMessageReceived(bc *BridgedConnection, msg TDSMessage) {
	if ba.tDSMessageReceivedHandler != nil {
		ba.tDSMessageReceivedHandler(bc, msg)
	}
}

// onTDSPacketReceived 触发TDS数据包接收事件
func (ba *BridgeAcceptor) onTDSPacketReceived(bc *BridgedConnection, packet *TDSPacket) {
	if ba.tDSPacketReceivedHandler != nil {
		ba.tDSPacketReceivedHandler(bc, packet)
	}
}

// onConnectionAccepted 触发连接接受事件
func (ba *BridgeAcceptor) onConnectionAccepted(conn net.Conn) {
	if ba.connectionAcceptedHandler != nil {
		ba.connectionAcceptedHandler(conn)
	}
}

// onListeningThreadException 触发监听线程异常事件
func (ba *BridgeAcceptor) onListeningThreadException(listener net.Listener, err error) {
	if ba.listeningThreadExceptionHandler != nil {
		ba.listeningThreadExceptionHandler(listener, err)
	}
}

// onBridgeException 触发桥接异常事件
func (ba *BridgeAcceptor) onBridgeException(bc *BridgedConnection, ct ConnectionType, err error) {
	if ba.bridgeExceptionHandler != nil {
		ba.bridgeExceptionHandler(bc, ct, err)
	}
}

// onConnectionDisconnected 触发连接断开事件
func (ba *BridgeAcceptor) onConnectionDisconnected(bc *BridgedConnection, ct ConnectionType) {
	if ba.connectionDisconnectedHandler != nil {
		ba.connectionDisconnectedHandler(bc, ct)
	}
}

// BridgedConnection 桥接连接结构体
type BridgedConnection struct {
	BridgeAcceptor *BridgeAcceptor
	SocketCouple   *SocketCouple
	mu             sync.Mutex
}

// NewBridgedConnection 创建新的BridgedConnection
func NewBridgedConnection(bridgeAcceptor *BridgeAcceptor, socketCouple *SocketCouple) *BridgedConnection {
	return &BridgedConnection{
		BridgeAcceptor: bridgeAcceptor,
		SocketCouple:   socketCouple,
	}
}

// Start 启动桥接连接
func (bc *BridgedConnection) Start() {
	// 启动客户端到SQL Server的goroutine
	go bc.clientBridgeToSQLServer()
	// 启动SQL Server到客户端的goroutine
	go bc.sqlServerToClientBridge()
}

// clientBridgeToSQLServer 处理从客户端到SQL Server的数据传输
func (bc *BridgedConnection) clientBridgeToSQLServer() {
	defer func() {
		bc.onConnectionDisconnected(ClientBridge)
	}()

	var bBuffer []byte
	bHeader := make([]byte, HEADER_SIZE)
	var tdsMessage TDSMessage

	for {
		// 接收头部
		n, err := bc.SocketCouple.ClientBridgeSocket.Read(bHeader)
		if err != nil || n == 0 {
			bc.onBridgeException(ClientBridge, err)
			return
		}

		// 创建TDS头部
		header := NewTDSHeader(bHeader)

		// 确保缓冲区大小足够
		minBufferSize := max(0x1000, header.LengthIncludingHeader()+1)
		if bBuffer == nil || len(bBuffer) < minBufferSize {
			bBuffer = make([]byte, minBufferSize)
		}

		// 接收有效载荷
		var received int
		if header.Type() == HeaderType(23) {
			received, err = bc.SocketCouple.ClientBridgeSocket.Read(bBuffer[:0x1000-HEADER_SIZE])
		} else if header.PayloadSize() > 0 {
			received, err = bc.SocketCouple.ClientBridgeSocket.Read(bBuffer[:header.PayloadSize()])
		}

		if err != nil {
			bc.onBridgeException(ClientBridge, err)
			return
		}

		// 创建TDS数据包
		tdsPacket := NewTDSPacket(bHeader, bBuffer, header.PayloadSize())
		
		// 触发数据包接收事件
		bc.onTDSPacketReceived(tdsPacket)

		// 构建消息
		if tdsMessage == nil {
			tdsMessage = CreateTDSMessageFromFirstPacket(tdsPacket)
		} else {
			tdsMessage.AddPacket(tdsPacket)
		}

		// 检查消息是否完成
		if (header.StatusBitMask() & END_OF_MESSAGE) == END_OF_MESSAGE {
			bc.onTDSMessageReceived(tdsMessage)
			tdsMessage = nil
		}

		// 发送头部到SQL Server
		_, err = bc.SocketCouple.BridgeSQLSocket.Write(bHeader)
		if err != nil {
			bc.onBridgeException(ClientBridge, err)
			return
		}

		// 发送有效载荷到SQL Server
		if header.Type() == HeaderType(23) {
			_, err = bc.SocketCouple.BridgeSQLSocket.Write(bBuffer[:received])
		} else {
			_, err = bc.SocketCouple.BridgeSQLSocket.Write(bBuffer[:header.PayloadSize()])
		}

		if err != nil {
			bc.onBridgeException(ClientBridge, err)
			return
		}
	}
}

// sqlServerToClientBridge 处理从SQL Server到客户端的数据传输
func (bc *BridgedConnection) sqlServerToClientBridge() {
	defer func() {
		bc.onConnectionDisconnected(BridgeSQL)
	}()

	bBuffer := make([]byte, 4096)

	for {
		// 接收数据
		n, err := bc.SocketCouple.BridgeSQLSocket.Read(bBuffer)
		if err != nil || n == 0 {
			bc.onBridgeException(BridgeSQL, err)
			return
		}

		// 发送数据到客户端
		_, err = bc.SocketCouple.ClientBridgeSocket.Write(bBuffer[:n])
		if err != nil {
			bc.onBridgeException(BridgeSQL, err)
			return
		}
	}
}

// onTDSMessageReceived 触发TDS消息接收事件
func (bc *BridgedConnection) onTDSMessageReceived(msg TDSMessage) {
	bc.BridgeAcceptor.onTDSMessageReceived(bc, msg)
}

// onTDSPacketReceived 触发TDS数据包接收事件
func (bc *BridgedConnection) onTDSPacketReceived(packet *TDSPacket) {
	bc.BridgeAcceptor.onTDSPacketReceived(bc, packet)
}

// onBridgeException 触发桥接异常事件
func (bc *BridgedConnection) onBridgeException(ct ConnectionType, err error) {
	bc.BridgeAcceptor.onBridgeException(bc, ct, err)
}

// onConnectionDisconnected 触发连接断开事件并关闭相应的连接
func (bc *BridgedConnection) onConnectionDisconnected(ct ConnectionType) {
	bc.BridgeAcceptor.onConnectionDisconnected(bc, ct)

	bc.mu.Lock()
	defer bc.mu.Unlock()

	switch ct {
	case ClientBridge:
		if bc.SocketCouple.BridgeSQLSocket != nil {
			bc.SocketCouple.BridgeSQLSocket.Close()
		}
	case BridgeSQL:
		if bc.SocketCouple.ClientBridgeSocket != nil {
			bc.SocketCouple.ClientBridgeSocket.Close()
		}
	}
}

// max 返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}