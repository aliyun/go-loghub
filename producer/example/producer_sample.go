package main

import (
	"fmt"
	. "github.com/aliyun/aliyun-log-go-sdk"
	. "github.com/aliyun/aliyun-log-go-sdk/producer"
	"github.com/gogo/protobuf/proto"
	"math/rand"
	"time"
)

var project = &LogProject{
	Name:            "test2222222222",
	Endpoint:        "cn-beijing.log.aliyuncs.com",
	AccessKeyID:     "ak-xxx",
	AccessKeySecret: "sk-xxx",
}

func main() {
	fmt.Println("loghub sample begin")

	begin_time := uint32(time.Now().Unix())
	rand.Seed(int64(begin_time))
	logstore_name := "sls-test"
	logstore_name2 := "sls-test-2"

	project_pool := &ProjectPool{}
	project_pool.UpdateProject(project)

	producer := LogProducer{}
	producer.Init(project_pool)
	defer producer.Destroy()

	// put logs to logstore
	for loggroupIdx := 0; loggroupIdx < 3; loggroupIdx++ {
		logs := []*Log{}
		for logIdx := 0; logIdx < 10; logIdx++ {
			content := []*LogContent{}
			for colIdx := 0; colIdx < 10; colIdx++ {
				content = append(content, &LogContent{
					Key:   proto.String(fmt.Sprintf("col_%d", colIdx)),
					Value: proto.String(fmt.Sprintf("loggroup idx: %d, log idx: %d, col idx: %d, value: %d", loggroupIdx, logIdx, colIdx, rand.Intn(10000000))),
				})
			}
			log := &Log{
				Time:     proto.Uint32(uint32(time.Now().Unix())),
				Contents: content,
			}
			logs = append(logs, log)
		}
		loggroup := &LogGroup{
			Topic:  proto.String("test-bing"),
			Source: proto.String("10.230.201.117"),
			Logs:   logs,
		}

		err := producer.Send(project.Name, logstore_name, "", loggroup, nil)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = producer.Send(project.Name, logstore_name2, "", loggroup, nil)
		if err != nil {
			fmt.Println(err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(5 * time.Second)

	fmt.Println("loghub sample end")
}
