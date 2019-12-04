package main

import (
	"fmt"
	"github.com/aliyun/aliyun-log-go-sdk/producer"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	producerConfig := producer.GetDefaultProducerConfig()
	producerConfig.Endpoint = os.Getenv("Endpoint")
	producerConfig.AccessKeyID = os.Getenv("AccessKeyID")
	producerConfig.AccessKeySecret = os.Getenv("AccessKeySecret")
	producerConfig.SecurityToken = ""
	producerInstance := producer.InitProducer(producerConfig)
	ch := make(chan os.Signal)
	signal.Notify(ch)
	producerInstance.Start()
	var m sync.WaitGroup
	for i := 0; i < 10; i++ {
		m.Add(1)
		go func() {
			defer m.Done()
			for i := 0; i < 1000; i++ {
				//Call the PutNewProducerConfig function to pass a new sts token.
				if i == 500 {
					time.Sleep(10 * time.Second)
					// note : please configure your correct ste token value.
					producerConfig.SecurityToken = "your new sts token"
					producer.PutNewProducerConfig(producerConfig)
				}
				// GenerateLog  is producer's function for generating SLS format logs
				// GenerateLog has low performance, and native Log interface is the best choice for high performance.
				log := producer.GenerateLog(uint32(time.Now().Unix()), map[string]string{"content": "test", "content2": fmt.Sprintf("%v", i)})
				err := producerInstance.SendLog("project", "logstore", "topic", "127.0.0.1", log)
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
		producerInstance.Close(60)
	}
}
