package sls_producer

import (
	"fmt"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/base"
	. "github.com/aliyun/aliyun-log-go-sdk/producer/config"
	"log"
	"strings"
	"sync"
	"time"
)

type IOWorker struct {
	queue  chan *PackageData
	wg     sync.WaitGroup
	config *ProducerConfig
}

func (worker *IOWorker) AddPackage(data *PackageData, bytes int) {
	worker.queue <- data
	log.Printf("IOWorker: success to put logs to producer queue, %v\n", bytes)
}

func (i *IOWorker) Close() {
	close(i.queue)
	i.wg.Wait()
}

func (worker *IOWorker) Init(config *ProducerConfig) {
	worker.queue = make(chan *PackageData)
	worker.config = config

	for i := 0; i < 5; i++ {
		flag := fmt.Sprintf("No.%d", i)
		worker.wg.Add(1)
		go sendToServer(worker, flag)
	}
}

func sendToServer(worker *IOWorker, flag string) {
	defer worker.wg.Done()

	log.Printf("%s go routine start\n", flag)
	for data := range worker.queue {
		log.Printf("%s: put logs to sls %s , %d\n", flag, data.LogstoreName, data.LogGroup.Size())

		for retry_times := 0; retry_times < worker.config.RetryTimes; retry_times++ {
			err := data.Logstore.PutLogs(data.LogGroup)
			if err == nil {
				log.Printf("PutLogs success, retry: %d\n", retry_times)
				break
			} else {
				log.Printf("PutLogs fail, retry: %d, err: %s\n", retry_times, err)
				//handle exception here, you can add retryable erorrCode, set appropriate put_retry
				if strings.Contains(err.Error(), sls.WRITE_QUOTA_EXCEED) || strings.Contains(err.Error(), sls.PROJECT_QUOTA_EXCEED) || strings.Contains(err.Error(), sls.SHARD_WRITE_QUOTA_EXCEED) {
					//maybe you should split shard
					time.Sleep(1000 * time.Millisecond)
				} else if strings.Contains(err.Error(), sls.INTERNAL_SERVER_ERROR) || strings.Contains(err.Error(), sls.SERVER_BUSY) {
					time.Sleep(200 * time.Millisecond)
				}
			}
		}

	}

	log.Printf("%s go routine exit\n", flag)
}
