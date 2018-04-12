package main

import (
	"fmt"
	. "github.com/aliyun/aliyun-log-go-sdk"
	. "github.com/aliyun/aliyun-log-go-sdk/producer"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/callback"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/config"
	"github.com/gogo/protobuf/proto"
	"log"
	"math/rand"
	"sync"
	"time"
)

var project = &LogProject{
	Name:            "test2222222222",
	Endpoint:        "cn-beijing.log.aliyuncs.com",
	AccessKeyID:     "LTAIrapwKlEFaxKv",
	AccessKeySecret: "1u0WHu1t6wrMM8TXY5SHgjO0ON77Hk",
}

func main() {
	log.Println("loghub sample begin")

	begin_time := uint32(time.Now().Unix())
	rand.Seed(int64(begin_time))
	logstore_name := "sls-test"
	// logstore_name2 := "sls-test-2"

	project_pool := &ProjectPool{}
	project_pool.UpdateProject(project)

	producer := LogProducer{}
	producer.Init(project_pool, &DefaultGlobalProducerConfig)
	defer producer.Destroy()

	wg := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		// go sendLogs(&producer, logstore_name, fmt.Sprintf("test-log-%d", i), fmt.Sprintf("10.0.0.%d", i))
		go sendLogs(&producer, logstore_name, fmt.Sprintf("test-log"), fmt.Sprintf("10.0.0.0"), wg)
	}

	wg.Wait()
	log.Println("loghub sample end")
	time.Sleep(50 * time.Second)
}

func sendLogs(producer *LogProducer, logstore_name string, topic string, source string, wg *sync.WaitGroup) {
	// put logs to logstore
	for loggroupIdx := 0; loggroupIdx < 100; loggroupIdx++ {
		logs := []*Log{}
		for logIdx := 0; logIdx < 2; logIdx++ {
			content := []*LogContent{}
			for colIdx := 0; colIdx < 2; colIdx++ {
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
			Topic:  proto.String(topic),
			Source: proto.String(source),
			Logs:   logs,
		}

		err := producer.Send(project.Name, logstore_name, "", loggroup, &DefaultLogCallback{})
		if err != nil {
			log.Println(err)
			return
		}

		time.Sleep(5 * time.Millisecond)
		log.Println("log routine end, ", topic, source)
	}

	wg.Done()
}
