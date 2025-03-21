package main

import (
	"fmt"
	"gazer-agent/common"
	"gazer-agent/etcd"
	"gazer-agent/kafka"
	"gazer-agent/tailfile"
	"github.com/sirupsen/logrus"
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
func Run() {
	// 获取本机ip
	ip, err := common.GetOutboundIp()
	if err != nil {
		logrus.Errorf("get ip failed, err:%v", err)
		return
	}
	fmt.Println(ip)

	var configObj = new(Config)
	// 读配置文件
	err = ini.MapTo(configObj, "conf/config.ini")
	if err != nil {
		logrus.Errorf("load config failed, err:%v", err)
		return
	}
	fmt.Printf("%+v\n", configObj)

	// 初始化，连接kafka
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed, err:%v\n", err)
		return
	}
	logrus.Info("init kafka success!")

	// 初始化etcd连接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed, err:%v\n", err)
		return
	}
	// 从etcd中拉去要收集的日志的配置项
	allConf, err := etcd.GerConf(ip)
	if err != nil {
		logrus.Errorf("get conf from etcd failed, err:%v", err)
		return
	}
	fmt.Printf("%+v\n", allConf)
	// 使用watch去监控etcd中 add、upd、del 的变化
	watchEtcd()

	// 根据配置中的路径初始化tail
	err = tailfile.Init(allConf) // 把从etcd中获取的配置项传到Init中
	if err != nil {
		logrus.Errorf("init tail failed, err:%v\n", err)
		return
	}
	logrus.Info("init tailfile success!")

	// 把日志通过sarama发往kafka
	run()

}
