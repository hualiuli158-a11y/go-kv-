package main

func main() {
	// 1. 初始化线程安全的跳表引擎 (注意这里类型改为了 string, string)
	db := NewSkipList[string, string]()

	// 2. 启动阻塞的网络服务
	StartServer("8088", db)
}
