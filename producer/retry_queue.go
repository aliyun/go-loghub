package producer

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"container/heap"
	"time"
)

type RetryQueue []*ProducerBatch

func (retryQueue *RetryQueue) sendToRetryQueue(producerBatch *ProducerBatch, logger log.Logger) {
	level.Debug(logger).Log("msg", "Retry queue to get data")
	if producerBatch != nil {
		heap.Push(retryQueue, producerBatch)
	}
}

func (retryQueue *RetryQueue) getRetryBatch(moverShutDownFlag bool) (producerBatchList []*ProducerBatch) {
	if !moverShutDownFlag {
		for retryQueue.Len() > 0 {
			producerBatch := heap.Pop(retryQueue)
			if producerBatch.(*ProducerBatch).nextRetryMs < GetTimeMs(time.Now().UnixNano()) {
				producerBatchList = append(producerBatchList, producerBatch.(*ProducerBatch))
			} else {
				heap.Push(retryQueue, producerBatch.(*ProducerBatch))
				break
			}
		}
	} else {
		for retryQueue.Len() > 0 {
			producerBatch := heap.Pop(retryQueue)
			producerBatchList = append(producerBatchList, producerBatch.(*ProducerBatch))
		}
	}
	return producerBatchList
}

func initRetryQueue() *RetryQueue {
	retryQueue := RetryQueue{}
	heap.Init(&retryQueue)
	return &retryQueue
}

func (retryQueue RetryQueue) Len() int {
	return len(retryQueue)
}

func (retryQueue RetryQueue) Less(i, j int) bool {
	return retryQueue[i].nextRetryMs < retryQueue[j].nextRetryMs
}
func (retryQueue RetryQueue) Swap(i, j int) {
	retryQueue[i], retryQueue[j] = retryQueue[j], retryQueue[i]
}
func (retryQueue *RetryQueue) Push(x interface{}) {
	item := x.(*ProducerBatch)
	*retryQueue = append(*retryQueue, item)
}
func (retryQueue *RetryQueue) Pop() interface{} {
	old := *retryQueue
	n := len(old)
	item := old[n-1]
	*retryQueue = old[0 : n-1]
	return item
}
