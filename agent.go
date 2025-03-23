package agent

import (
	"fmt"
	"github.com/cdlicn/gazer-agent/common"
	"github.com/cdlicn/gazer-agent/etcd"
	"github.com/cdlicn/gazer-agent/kafka"
	"github.com/cdlicn/gazer-agent/tailfile"
	"gopkg.in/ini.v1"
)

type Config struct {
	KafkaConfig `ini:"kafka"`
	EtcdConfig  `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int64  `ini:"chan_size"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

func watchEtcd() {
	go etcd.WatchAdd()
	go etcd.WatchUpd()
	go etcd.WatchDel()
}

func run() {
	select {}
}

// func main() {
func Run() error {
	// 获取本机ip
	ip, err := common.GetOutboundIp()
	if err != nil {
		return fmt.Errorf("get ip failed, err:%v", err)
	}
	//fmt.Println(ip)

	var configObj = new(Config)
	// 读配置文件
	err = ini.MapTo(configObj, "conf/config.ini")
	if err != nil {
		return fmt.Errorf("load config failed, err:%v", err)
	}
	//fmt.Printf("%+v\n", configObj)

	// 初始化，连接kafka
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		return fmt.Errorf("init kafka failed, err:%v\n", err)
	}
	//logrus.Info("init kafka success!")

	// 初始化etcd连接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		return fmt.Errorf("init etcd failed, err:%v\n", err)
	}
	// 从etcd中拉去要收集的日志的配置项
	allConf, err := etcd.GerConf(ip)
	if err != nil {
		return fmt.Errorf("get conf from etcd failed, err:%v", err)
	}
	//fmt.Printf("%+v\n", allConf)

	// 根据配置中的路径初始化tail
	err = tailfile.Init(allConf) // 把从etcd中获取的配置项传到Init中
	if err != nil {
		return fmt.Errorf("init tail failed, err:%v\n", err)
	}
	//logrus.Info("init tailfile success!")

	// 使用watch去监控etcd中 add、upd、del 的变化
	watchEtcd()

	// 把日志通过sarama发往kafka
	run()

	return nil
}
