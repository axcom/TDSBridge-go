package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/axcom/tdsbridge-go/pkg"
)

func main() {
	if len(os.Args) < 4 {
		usage()
		return
	}

	listenPort := os.Args[1]
	sqlServerAddr := os.Args[2]
	sqlServerPort := os.Args[3]

	// 解析SQL Server地址
	iphe, err := net.LookupHost(sqlServerAddr)
	if err != nil {
		fmt.Printf("Error resolving SQL Server address: %v\n", err)
		return
	}

	sqlServerEndpoint := net.JoinHostPort(iphe[0], sqlServerPort)

	// 创建BridgeAcceptor
	bridgeAcceptor := pkg.NewBridgeAcceptor(listenPort, sqlServerEndpoint)

	// 设置事件处理函数
	bridgeAcceptor.SetTDSMessageReceivedHandler(handleTDSMessageReceived)
	bridgeAcceptor.SetTDSPacketReceivedHandler(handleTDSPacketReceived)
	bridgeAcceptor.SetConnectionAcceptedHandler(handleConnectionAccepted)
	bridgeAcceptor.SetConnectionDisconnectedHandler(handleConnectionDisconnected)

	// 启动桥接器
	err = bridgeAcceptor.Start()
	if err != nil {
		fmt.Printf("Error starting bridge: %v\n", err)
		return
	}

	fmt.Println("Press enter to kill this process...")
	var input string
	fmt.Scanln(&input)

	// 停止桥接器
	bridgeAcceptor.Stop()
}

func handleConnectionDisconnected(bc *pkg.BridgedConnection, ct pkg.ConnectionType) {
	fmt.Printf("%s|Connection %s closed (%s)\n", formatDateTime(), ct, bc.SocketCouple)
}

func handleConnectionAccepted(s net.Conn) {
	fmt.Printf("%s|New connection from %s\n", formatDateTime(), s.RemoteAddr())
}

func handleTDSPacketReceived(bc *pkg.BridgedConnection, packet *pkg.TDSPacket) {
	fmt.Printf("%s|%s\n", formatDateTime(), packet)
}

// 包级别的原子计数器，确保在多 goroutine 环境下生成唯一文件名
var iRPC uint64

func handleTDSMessageReceived(bc *pkg.BridgedConnection, msg pkg.TDSMessage) {
	fmt.Printf("%s|%s\n", formatDateTime(), msg)

	// 处理SQLBatchMessage
	if sqlBatchMsg, ok := msg.(*pkg.SQLBatchMessage); ok {
		strBatchText := sqlBatchMsg.GetBatchText()
		// 注意：Go 中字符串的长度是按字节计算的，与 C# 中按字符（UTF-16 码元）计算不同。
		// 如果 GetBatchText 返回的是纯 ASCII 字符串，长度一致。否则需要调整。
		fmt.Printf("\tSQLBatch message (%d chars worth of %d bytes of data)[%s]\n",
			len(strBatchText), len(strBatchText)*2, strBatchText)
	} else if rpcMsg, ok := msg.(*pkg.RPCRequestMessage); ok {
		// 处理RPCRequestMessage
		// 这里可以添加额外的RPC消息处理逻辑

		// +build debug
		{
			// 使用匿名函数和 defer/recover 来模拟 try-catch 块
			func() {
				defer func() {
					if r := recover(); r != nil {
						// 捕获到恐慌（Panic），对应 C# 的 catch (Exception)
						fmt.Printf("Exception: %v\n", r)
					}
				}()
				bPayload := rpcMsg.AssemblePayload()

				// 使用原子操作递增计数器，确保文件名唯一
				fileName := filepath.Join(".\\dev", strconv.FormatUint(atomic.AddUint64(&iRPC, 1), 10)+".raw")
				// 打开文件，设置为创建、只写模式
				fs, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					// 错误处理
					fmt.Printf("Failed to create file %s: %v", fileName, err)
					return
				}
				// 使用 defer 确保文件在函数退出时被关闭
				defer fs.Close()
				// 将 payload 写入文件
				_, err = fs.Write(bPayload)
				if err != nil {
					// 错误处理
					fmt.Printf("Failed to write to file %s: %v", fileName, err)
					return
				}
				fmt.Printf("Write to file %s: %v", fileName, err)

			}() // 使用匿名函数和 defer/recover 来模拟 try-catch 块
		}
		// -build debug
	}
	// 可以添加更多 else if 分支来处理其他消息类型
}

func formatDateTime() string {
	return time.Now().Format("2006-01-02 15:04:05.000000")
}

func usage() {
	fmt.Println("TDSBridge <listen port> <sql server address> <sql server port>")
}
