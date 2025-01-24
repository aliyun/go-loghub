package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/aliyun/aliyun-log-go-sdk/producer"
)

type Callback struct {
}

func (callback *Callback) Success(result *producer.Result) {
	attemptList := result.GetReservedAttempts()
	for _, attempt := range attemptList {
		fmt.Println(attempt)
	}
}

func (callback *Callback) Fail(result *producer.Result) {
	fmt.Println(result.IsSuccessful())
	fmt.Println(result.GetErrorCode())
	fmt.Println(result.GetErrorMessage())
	fmt.Println(result.GetReservedAttempts())
	fmt.Println(result.GetRequestId())
	fmt.Println(result.GetTimeStampMs())
}

func main() {
	producerConfig := producer.GetDefaultProducerConfig()
	producerConfig.Endpoint = os.Getenv("Endpoint")
	producerConfig.AccessKeyID = os.Getenv("AccessKeyID")
	producerConfig.AccessKeySecret = os.Getenv("AccessKeySecret")
	producerInstance, err := producer.NewProducer(producerConfig)
	if err != nil {
		panic(err)
	}
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Kill, os.Interrupt)
	producerInstance.Start()
	var m sync.WaitGroup
	callBack := &Callback{}
	for i := 0; i < 10; i++ {
		m.Add(1)
		go func() {
			defer m.Done()
			for i := 0; i < 1000; i++ {
				// GenerateLog  is producer's function for generating SLS format logs
				// GenerateLog has low performance, and native Log interface is the best choice for high performance.
				log := producer.GenerateLog(uint32(time.Now().Unix()), map[string]string{"content": "test", "content2": fmt.Sprintf("%v", i)})
				err := producerInstance.SendLogWithCallBack("project", "logstrore", "topic", "127.0.0.1", log, callBack)
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
