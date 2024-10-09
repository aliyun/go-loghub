package consumerLibrary

import (
	"io"
	"os"
	"sync"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/atomic"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type ConsumerWorker struct {
	consumerHeatBeat   *ConsumerHeartBeat
	client             *ConsumerClient
	workerShutDownFlag *atomic.Bool
	shardConsumer      sync.Map // map[int]*ShardConsumerWorker
	processor          Processor
	waitGroup          sync.WaitGroup
	Logger             log.Logger
}

// depreciated: this old logic is to automatically save to memory, and then commit at a fixed time
// we highly recommend you to use InitConsumerWorkerWithCheckpointTracker
func InitConsumerWorker(option LogHubConfig, do func(int, *sls.LogGroupList) string) *ConsumerWorker {
	if option.AutoCommitDisabled {
		panic("auto commit already disabled, sdk will not save any checkpoint, " +
			"please use InitConsumerWorkerWithCheckpointTracker or set AutoCommitDisabled to false")
	}
	return InitConsumerWorkerWithProcessor(
		option,
		ProcessFunc(func(shardId int, logGroupList *sls.LogGroupList, checkpointTracker CheckPointTracker) (string, error) {
			cursor := do(shardId, logGroupList)
			// keep the original logic
			// if cursor is not empty, we don't save,
			if cursor == "" {
				checkpointTracker.SaveCheckPoint(false)
			}
			return cursor, nil
		}),
	)
}

// InitConsumerWorkerWithCheckpointTracker
// please note that you need to save after the process is successful，
func InitConsumerWorkerWithCheckpointTracker(option LogHubConfig, do func(int, *sls.LogGroupList, CheckPointTracker) (string, error)) *ConsumerWorker {
	return InitConsumerWorkerWithProcessor(option, ProcessFunc(do))
}

// InitConsumerWorkerWithProcessor
// you need save checkpoint by yourself and can do something after consumer shutdown
func InitConsumerWorkerWithProcessor(option LogHubConfig, processor Processor) *ConsumerWorker {
	logger := option.Logger
	if logger == nil {
		logger = logConfig(option)
	}

	consumerClient := initConsumerClient(option, logger)
	consumerHeatBeat := initConsumerHeatBeat(consumerClient, logger)
	consumerWorker := &ConsumerWorker{
		consumerHeatBeat:   consumerHeatBeat,
		client:             consumerClient,
		workerShutDownFlag: atomic.NewBool(false),
		//shardConsumer:      make(map[int]*ShardConsumerWorker),
		processor: processor,
		Logger:    logger,
	}
	if err := consumerClient.createConsumerGroup(); err != nil {
		level.Error(consumerWorker.Logger).Log(
			"msg", "possibly failed to create or update consumer group, please check worker run log",
			"err", err)
	}
	return consumerWorker
}

func (consumerWorker *ConsumerWorker) Start() {
	consumerWorker.waitGroup.Add(1)
	go consumerWorker.run()
}

func (consumerWorker *ConsumerWorker) StopAndWait() {
	level.Info(consumerWorker.Logger).Log("msg", "*** try to exit ***")
	consumerWorker.workerShutDownFlag.Store(true)
	consumerWorker.consumerHeatBeat.shutDownHeart()
	consumerWorker.waitGroup.Wait()
	level.Info(consumerWorker.Logger).Log("msg", "consumer worker stopped", "consumer name", consumerWorker.client.option.ConsumerName)
}

func (consumerWorker *ConsumerWorker) run() {
	level.Info(consumerWorker.Logger).Log("msg", "consumer worker start", "worker name", consumerWorker.client.option.ConsumerName)
	defer consumerWorker.waitGroup.Done()
	go consumerWorker.consumerHeatBeat.heartBeatRun()

	for !consumerWorker.workerShutDownFlag.Load() {
		heldShards := consumerWorker.consumerHeatBeat.getHeldShards()
		lastFetchTime := time.Now().UnixNano() / 1000 / 1000

		for _, shard := range heldShards {
			if consumerWorker.workerShutDownFlag.Load() {
				break
			}
			shardConsumer := consumerWorker.getShardConsumer(shard)
			shardConsumer.consume()
		}
		consumerWorker.cleanShardConsumer(heldShards)
		TimeToSleepInMillsecond(consumerWorker.client.option.DataFetchIntervalInMs, lastFetchTime, consumerWorker.workerShutDownFlag.Load())

	}
	level.Info(consumerWorker.Logger).Log("msg", "consumer worker try to cleanup consumers", "worker name", consumerWorker.client.option.ConsumerName)
	consumerWorker.shutDownAndWait()
}

func (consumerWorker *ConsumerWorker) shutDownAndWait() {
	for {
		time.Sleep(500 * time.Millisecond)
		count := 0
		consumerWorker.shardConsumer.Range(
			func(key, value interface{}) bool {
				count++
				consumer := value.(*ShardConsumerWorker)
				if !consumer.isShutDownComplete() {
					consumer.consumerShutDown()
				} else {
					consumerWorker.shardConsumer.Delete(key)
				}
				return true
			},
		)
		if count == 0 {
			break
		}
	}

}

func (consumerWorker *ConsumerWorker) getShardConsumer(shardId int) *ShardConsumerWorker {
	consumer, ok := consumerWorker.shardConsumer.Load(shardId)
	if ok {
		return consumer.(*ShardConsumerWorker)
	}
	consumerIns := initShardConsumerWorker(shardId, consumerWorker.client, consumerWorker.consumerHeatBeat, consumerWorker.processor, consumerWorker.Logger)
	consumerWorker.shardConsumer.Store(shardId, consumerIns)
	return consumerIns

}

func (consumerWorker *ConsumerWorker) cleanShardConsumer(owned_shards []int) {

	consumerWorker.shardConsumer.Range(
		func(key, value interface{}) bool {
			shard := key.(int)
			consumer := value.(*ShardConsumerWorker)

			if !Contain(shard, owned_shards) {
				level.Info(consumerWorker.Logger).Log("msg", "try to call shut down for unassigned consumer shard", "shardId", shard)
				consumer.consumerShutDown()
				level.Info(consumerWorker.Logger).Log("msg", "Complete call shut down for unassigned consumer shard", "shardId", shard)
			}

			if consumer.isShutDownComplete() {
				isDeleteShard := consumerWorker.consumerHeatBeat.removeHeartShard(shard)
				if isDeleteShard {
					level.Info(consumerWorker.Logger).Log("msg", "Remove an assigned consumer shard", "shardId", shard)
					consumerWorker.shardConsumer.Delete(shard)
				} else {
					level.Info(consumerWorker.Logger).Log("msg", "Remove an assigned consumer shard failed", "shardId", shard)
				}
			}
			return true
		},
	)
}

// This function is used to initialize the global logger
func logConfig(option LogHubConfig) log.Logger {
	var writer io.Writer
	if option.LogFileName == "" {
		writer = log.NewSyncWriter(os.Stdout)
	} else {
		if option.LogMaxSize == 0 {
			option.LogMaxSize = 10
		}
		if option.LogMaxBackups == 0 {
			option.LogMaxBackups = 10
		}
		writer = &lumberjack.Logger{
			Filename:   option.LogFileName,
			MaxSize:    option.LogMaxSize,
			MaxBackups: option.LogMaxBackups,
			Compress:   option.LogCompass,
		}
	}
	var logger log.Logger
	if option.IsJsonType {
		logger = log.NewJSONLogger(writer)
	} else {
		logger = log.NewLogfmtLogger(writer)
	}
	switch option.AllowLogLevel {
	case "debug":
		logger = level.NewFilter(logger, level.AllowDebug())
	case "info":
		logger = level.NewFilter(logger, level.AllowInfo())
	case "warn":
		logger = level.NewFilter(logger, level.AllowWarn())
	case "error":
		logger = level.NewFilter(logger, level.AllowError())
	default:
		logger = level.NewFilter(logger, level.AllowInfo())
	}
	logger = log.With(logger, "time", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	return logger
}
