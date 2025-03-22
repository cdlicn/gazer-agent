package etcd

import (
	"context"
	"fmt"
	"github.com/cdlicn/gazer-agent/common"
	"github.com/cdlicn/gazer-agent/tailfile"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Printf("connect to etcd failed, error:%v", err)
		return
	}
	return
}

func GerConf(ip string) (collectEntry []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	resp, err := client.Get(ctx, ip, clientv3.WithPrefix())
	if err != nil {
		logrus.Errorf("failed to connect etcd, %v", err)
		return
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len:0 topic from etcd.")
		return
	}
	collectEntry = make([]common.CollectEntry, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		collectEntry[i].Topic = string(kv.Key)
		collectEntry[i].Path = string(kv.Value)
	}
	return
}

// 监控etcd中日志收集项配置变化的函数
func WatchAdd() {
	for {
		watchCh := client.Watch(context.Background(), "add")
		wresp := <-watchCh
		logrus.Infof("add new conf from etcd")
		topic := wresp.Events[0].Kv.Value
		getResp, err := client.Get(context.Background(), string(topic))
		if err != nil {
			logrus.Errorf("get topic from etcd failed, %v", err)
			continue
		}
		path := getResp.Kvs[0].Value
		conf := common.CollectEntry{
			Topic: string(topic),
			Path:  string(path),
		}
		tailfile.AddNewTailTask(conf)
	}
}

func WatchUpd() {
	for {
		watchCh := client.Watch(context.Background(), "upd")
		wresp := <-watchCh
		logrus.Infof("update a conf from etcd")
		topic := wresp.Events[0].Kv.Value
		getResp, err := client.Get(context.Background(), string(topic))
		if err != nil {
			logrus.Errorf("get topic from etcd failed, %v", err)
			continue
		}
		path := getResp.Kvs[0].Value
		conf := common.CollectEntry{
			Topic: string(topic),
			Path:  string(path),
		}
		tailfile.UpdTailTask(conf)
	}
}

func WatchDel() {
	for {
		watchCh := client.Watch(context.Background(), "del")
		wresp := <-watchCh
		logrus.Infof("delete a conf from etcd")
		topic := wresp.Events[0].Kv.Value
		tailfile.DelTailTask(string(topic))
	}
}
