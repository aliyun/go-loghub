package log_producer

import (
	"fmt"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/base"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/config"
	"log"
	"sync"
	"time"
)

type IOWorker struct {
	queue  chan *PackageData
	wg     sync.WaitGroup
	config *ProducerConfig
}

func (worker *IOWorker) AddPackage(data *PackageData, bytes int) {
	select {
	case worker.queue <- data:
		log.Printf("success to put logs to producer queue, %v\n", bytes)
	default:
		log.Println("producer queue exceed, failed to put logs to queue")
		data.Callback(BufferBusy{}, 0)
	}
}

func (i *IOWorker) Close() {
	close(i.queue)
	i.wg.Wait()
}

func (worker *IOWorker) Init(config *ProducerConfig) {
	worker.queue = make(chan *PackageData, config.IOWorkerCount)
	worker.config = config

	for i := 0; i < config.IOWorkerCount; i++ {
		flag := fmt.Sprintf("IO-Worker-%d", i)
		worker.wg.Add(1)
		go sendToServer(worker, flag)
	}
}

func sendToServer(worker *IOWorker, flag string) {
	log.Printf("%s go routine start\n", flag)
	defer worker.wg.Done()

	for data := range worker.queue {
		log.Printf("%s: put logs to aliyunlog %s , %d\n", flag, data.LogstoreName, data.LogGroup.Size())

		var err error
		for retry_times := 0; retry_times < worker.config.RetryTimes; retry_times++ {
			err = data.Logstore.PutLogs(data.LogGroup)
			if err == nil {
				log.Printf("%s: PutLogs success, retry: %d\n", flag, retry_times)
				break
			} else {
				log.Printf("%s: PutLogs fail, retry: %d, err: %s\n", flag, retry_times, err)
				time.Sleep(100 * time.Millisecond)
			}
		}

		if err != nil {
			data.Callback(err, 0)
		}
	}

	log.Printf("%s: go routine exit\n", flag)
}
