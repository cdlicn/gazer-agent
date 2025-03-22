package tailfile

import (
	"github.com/cdlicn/gazer-agent/common"
	"github.com/sirupsen/logrus"
)

// tailTask 的管理者
type tailTaskMgr struct {
	tailTaskMap      map[string]*tailTask  // 所有的tailTask任务 key: topic
	collectEntryList []common.CollectEntry // 所有配置项
}

var (
	ttMgr *tailTaskMgr
)

func Init(allConf []common.CollectEntry) (err error) {
	// allConf 存了若干个日志的收集项
	// 针对每一个日志收集项，创建一个对应的tailObj
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
	}
	for _, conf := range allConf {
		// 创建一个日志收集任务
		tt := newTailTask(conf.Topic, conf.Path)
		err := tt.Init()
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
			continue
		}
		logrus.Infof("create a tail task for path:%s success", conf.Path)
		ttMgr.tailTaskMap[tt.topic] = tt // 把创建的tailTask任务登记，方便后续管理
		// run
		go tt.run()
	}
	//go ttMgr.watch() // 在后台等新的配置
	return
}

func AddNewTailTask(conf common.CollectEntry) {
	tt := newTailTask(conf.Topic, conf.Path)
	err := tt.Init()
	if err != nil {
		logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
		return
	}
	logrus.Infof("create a tail task for path:%s success", conf.Path)
	ttMgr.tailTaskMap[tt.topic] = tt
	// run
	go tt.run()
}

func UpdTailTask(conf common.CollectEntry) {
	task := ttMgr.tailTaskMap[conf.Topic]
	task.cancel()
	err := task.tObj.Stop()
	if err != nil {
		logrus.Errorf("stop tailObj failed, err:%v", err)
	}

	tt := newTailTask(conf.Topic, conf.Path)
	err = tt.Init()
	if err != nil {
		logrus.Errorf("update tailObj for path:%s failed, err:%v", conf.Path, err)
		return
	}
	logrus.Infof("update a tail task for path:%s success", conf.Path)
	ttMgr.tailTaskMap[tt.topic] = tt
	// run
	go tt.run()
}

func DelTailTask(topic string) {
	task := ttMgr.tailTaskMap[topic]
	task.cancel()
	err := task.tObj.Stop()
	if err != nil {
		logrus.Errorf("stop tailObj failed, err:%v", err)
	}
	delete(ttMgr.tailTaskMap, topic) // 从管理类中删除
}
