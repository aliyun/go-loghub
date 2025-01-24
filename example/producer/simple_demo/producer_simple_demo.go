package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/producer"
	"github.com/gogo/protobuf/proto"
)

func main() {
	producerConfig := producer.GetDefaultProducerConfig()
	producerConfig.Endpoint = os.Getenv("Endpoint")
	producerConfig.AccessKeyID = os.Getenv("AccessKeyID")
	producerConfig.AccessKeySecret = os.Getenv("AccessKeySecret")
	// if you want to use log context, set generate pack id true
	producerConfig.GeneratePackId = true
	producerConfig.LogTags = []*sls.LogTag{
		&sls.LogTag{
			Key:   proto.String("tag_1"),
			Value: proto.String("value_1"),
		},
		&sls.LogTag{
			Key:   proto.String("tag_2"),
			Value: proto.String("value_2"),
		},
	}
	producerInstance, err := producer.NewProducer(producerConfig)
	if err != nil {
		panic(err)
	}
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Kill, os.Interrupt)
	producerInstance.Start()
	var m sync.WaitGroup
	for i := 0; i < 10; i++ {
		m.Add(1)
		go func() {
			defer m.Done()
			for i := 0; i < 1000; i++ {
				// GenerateLog  is producer's function for generating SLS format logs
				// GenerateLog has low performance, and native Log interface is the best choice for high performance.
				log := producer.GenerateLog(uint32(time.Now().Unix()), map[string]string{"content": "test", "content2": fmt.Sprintf("%v", i)})
				err := producerInstance.SendLog("log-project", "log-store", "topic", "127.0.0.1", log)
				if err != nil {
					fmt.Println(err)
				}
			}
		}()
	}
	m.Wait()
	fmt.Println("Send completion")
	if _, ok := <-ch; ok {
		fmt.Println("Get the shutdown signal and start to shut down")
		producerInstance.Close(60000)
	}
}
