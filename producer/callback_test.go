package producer

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"testing"
	"time"
)

type Callback struct {
	t *testing.T
}

func (callback *Callback) Success(result *Result) {
	attemptList := result.GetReservedAttempts()
	for _, attempt := range attemptList {
		fmt.Println(attempt)
	}
}

func (callback *Callback) Fail(result *Result) {
	if result.GetErrorMessage() == "" {
		callback.t.Error("Failed to get error message")
	}
	if result.GetErrorCode() == "" {
		callback.t.Error("Failed to get error code")
	}

	if len(result.GetReservedAttempts()) == 0 {
		callback.t.Error("Failed to get error code")
	}

}

func TestProducer_CallBack(t *testing.T) {
	producerConfig := GetDefaultProducerConfig()
	producerConfig.Endpoint = ""
	producerConfig.AccessKeyID = ""
	producerConfig.AccessKeySecret = ""
	producerInstance, err := NewProducer(producerConfig)
	if err != nil {
		panic(err)
	}
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Kill, os.Interrupt)
	producerInstance.Start()
	var m sync.WaitGroup
	callBack := &Callback{}
	for i := 0; i < 5; i++ {
		m.Add(1)
		go func() {
			defer m.Done()
			for i := 0; i < 10; i++ {
				log := GenerateLog(uint32(time.Now().Unix()), map[string]string{"content": "test", "content2": fmt.Sprintf("%v", i)})
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
