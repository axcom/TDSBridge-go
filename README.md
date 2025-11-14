# TDSBridge

TDSBridge是一个SQL Server TDS协议的桥接工具，用Go语言实现。它可以接收客户端的TDS请求，然后转发到SQL Server，并将SQL Server的响应返回给客户端。


## 项目结构

```
TDSBridge/
├── main.go          # 主程序入口
├── go.mod           # Go模块定义
├── pkg/
│   ├── connection.go # 连接管理相关代码
│   ├── header.go     # TDS头部相关代码
│   ├── packet.go     # TDS数据包相关代码
│   └── message.go    # TDS消息相关代码
└── README.md        # 项目说明文档
```

## 功能特性

- 支持SQL Server TDS协议的基本功能
- 可配置监听地址和端口
- 可配置目标SQL Server地址和端口
- 支持连接事件和消息事件的处理
- 支持TDS数据包和消息的解析和组装

## 编译和运行

### Windows

1. 确保已安装Go 1.20或更高版本
2. 或在命令行中执行以下命令：

```bash
# 编译
go build -o tdsbridge.exe .

# 运行
tdsbridge.exe <listen port> <sql server address> <sql server port>
```

### Linux/macOS

```bash
# 编译
go build -o tdsbridge .

# 运行(示例)
./tdsbridge 1433 192.168.1.111 1433
```

## 命令行参数

- `<listen port>`: 监听端口（SQL默认的是：1433）
- `<sql server address>`: SQL Server地址（真实MSSQL服务器IP地址）
- `<sql server port>`: SQL Server端口（真实MSSQL服务器端口,一般为：1433）
- `-help`: 显示帮助信息

## 日志和事件

程序会在控制台输出连接和消息相关的日志信息，包括：

- 新连接的建立
- 连接的断开
- TDS消息的接收
- TDS数据包的接收

## 注意事项

- 本项目是C#版本TDSBridge的Go语言重写版本，源自：https://github.com/MindFlavor/TDSBridge
- 使用前请确保目标SQL Server可正常访问
- 如需修改TDS协议的实现细节，请参考`pkg`目录下的相关文件

## 许可证

MIT