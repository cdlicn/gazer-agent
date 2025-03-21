package common

import (
	"net"
	"strings"
)

type CollectEntry struct {
	Topic string `json:"topic"` // 日志文件发往kafka中的topic
	Path  string `json:"path"`  // 日志文件存放的路径
}

// GetOutboundIp 获取本机Ip的函数
func GetOutboundIp() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip = strings.Split(localAddr.IP.String(), ":")[0]
	return
}
