package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// StartServer 启动 KV 数据库网络服务
func StartServer(port string, db *SkipList[string, string]) {
	// 1. 监听本地端口 (底层自动调用的就是 socket, bind, listen 和 epoll_ctl)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("启动服务失败: %v\n", err)
		return
	}
	defer listener.Close()
	fmt.Printf("KV 数据库已启动，正在监听端口 %s...\n", port)

	// 2. 主循环：不断接收新连接
	for {
		conn, err := listener.Accept() // 这里看起来是阻塞的，其实底层是被挂起的 Goroutine
		if err != nil {
			fmt.Printf("接收连接失败: %v\n", err)
			continue
		}

		// 3. 核心：来一个连接，直接丢给一个新的 Goroutine 处理！
		go handleConnection(conn, db)
	}
}

// handleConnection 处理单个客户端的生命周期
func handleConnection(conn net.Conn, db *SkipList[string, string]) {
	// 确保连接最终会关闭
	defer conn.Close()
	fmt.Printf("新客户端接入: %s\n", conn.RemoteAddr().String())

	// 使用 bufio 按行读取，自动处理了底层 socket 读缓冲区的粘包和拆包问题
	reader := bufio.NewReader(conn)

	for {
		// 按换行符读取一条完整指令
		line, err := reader.ReadString('\n')
		if err != nil {
			// 客户端断开或网络异常
			fmt.Printf("客户端断开: %s\n", conn.RemoteAddr().String())
			return
		}

		// 清理首尾空白和换行符
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 解析指令并执行
		response := executeCommand(line, db)

		// 将结果写回客户端
		conn.Write([]byte(response + "\n"))
	}
}

// executeCommand 解析协议并操作线程安全的跳表
func executeCommand(command string, db *SkipList[string, string]) string {
	parts := strings.SplitN(command, " ", 3) // 最多切分为3段: CMD KEY VALUE
	if len(parts) == 0 {
		return "ERROR: EMPTY_COMMAND"
	}

	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "SET":
		if len(parts) < 3 {
			return "ERROR: SET command requires key and value"
		}
		// 调用你写的并发安全跳表 Insert
		db.Insert(parts[1], parts[2])
		return "OK"

	case "GET":
		if len(parts) < 2 {
			return "ERROR: GET command requires key"
		}
		// 调用你写的并发安全跳表 Search
		if val, ok := db.Search(parts[1]); ok {
			return val
		}
		return "NOT_FOUND"

	case "DEL":
		if len(parts) < 2 {
			return "ERROR: DEL command requires key"
		}
		// 调用你写的并发安全跳表 Delete
		if db.Delete(parts[1]) {
			return "OK"
		}
		return "NOT_FOUND"

	default:
		return "ERROR: UNKNOWN_COMMAND"
	}
}
