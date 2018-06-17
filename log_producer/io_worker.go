package log_producer

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type IOWorker struct {
	queue  chan *PackageData
	wg     sync.WaitGroup
	config *ProducerConfig
}

func (worker *IOWorker) addPackage(data *PackageData) {
	data.SendToQueue = true

	select {
	case worker.queue <- data:
		log.Println("io worker received data successfully")
	default:
		log.Println("producer queue exceed, failed to put logs to queue")
		go data.Callback(BufferBusy{}, 0)
	}
}

func (i *IOWorker) Close() {
	log.Println("io worker start to close")
	close(i.queue)
	i.wg.Wait()
	log.Println("io worker closed successfully")
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
		log.Printf("%s: begin to put logs to aliyunlog %s , %d\n", flag, data.LogstoreName, data.LogGroup.Size())

		var err error
		for retry_times := 0; retry_times < worker.config.RetryTimes; retry_times++ {
			err = data.Logstore.PutLogs(data.LogGroup)
			if err == nil {
				break
			} else {
				log.Printf("%s: PutLogs fail, retry: %d, err: %s\n", flag, retry_times, err)
				time.Sleep(100 * time.Millisecond)
			}
		}

		if err != nil {
			data.Callback(err, 0)
		}

		log.Printf("%s: success to put logs to aliyunlog %s , %d\n", flag, data.LogstoreName, data.LogGroup.Size())
	}

	log.Printf("%s: go routine exit\n", flag)
}
