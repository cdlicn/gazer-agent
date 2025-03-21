package tailfile

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

// tail 相关

type tailTask struct {
	topic  string
	path   string
	tObj   *tail.Tail
	ctx    context.Context
	cancel context.CancelFunc
}

func newTailTask(topic, path string) *tailTask {
	ctx, cancel := context.WithCancel(context.Background())
	return &tailTask{
		topic:  topic,
		path:   path,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 实时跟踪
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件底部开始读取
		MustExist: false,                                // 文件不存在不报错
		Poll:      true,                                 // 轮询
	}
	t.tObj, err = tail.TailFile(t.path, cfg)
	return err
}

// 运行日志收集程序
func (t *tailTask) run() {
	// 读取日志，发往kafka
	logrus.Infof("collect for path:%s is running...", t.path)
	// logfile --> TailObj --> log --> client --> kafka
	for {
		select {
		case <-t.ctx.Done(): // 调用t.cancel() 关闭
			logrus.Infof("path:%s is stopping...", t.path)
			return
		case line, ok := <-t.tObj.Lines:
			if !ok {
				logrus.Warnf("tail file close reopen, path:%s\n", t.path)
				time.Sleep(time.Second)
				continue
			}
			// 如果是空行 则跳过
			if strings.TrimSpace(line.Text) == "" {
				continue
			}
			// 利用通道将同步的代码改为异步
			// 把读出来的一行日志包装成kafka里面的msg类型
			msg := &sarama.ProducerMessage{}
			msg.Topic = t.topic
			msg.Value = sarama.StringEncoder(line.Text)
			fmt.Printf("topic:%s, msg:%s\n", t.topic, line.Text)
			// TODO 放到通道中
			//kafka.ToMsgChan(msg)
		}
	}
}
