package consumerLibrary

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/atomic"
)

var shardLock sync.RWMutex

type ConsumerHeatBeat struct {
	Flag                     int
	client                   *ConsumerClient
	shutDownFlag             *atomic.Bool
	heldShards               []int
	heartShards              []int
	logger                   log.Logger
	lastHeartBeatSuccessTime int64
}

func initConsumerHeatBeat(consumerClient *ConsumerClient, logger log.Logger) *ConsumerHeatBeat {
	consumerHeatBeat := &ConsumerHeatBeat{
		client:                   consumerClient,
		shutDownFlag:             atomic.NewBool(false),
		heldShards:               []int{},
		heartShards:              []int{},
		logger:                   logger,
		lastHeartBeatSuccessTime: time.Now().Unix(),
	}
	return consumerHeatBeat
}

func (consumerHeatBeat *ConsumerHeatBeat) getHeldShards() []int {
	shardLock.RLock()
	defer shardLock.RUnlock()
	return consumerHeatBeat.heldShards
}

func (consumerHeatBeat *ConsumerHeatBeat) setHeldShards(heldShards []int) {
	shardLock.Lock()
	defer shardLock.Unlock()
	consumerHeatBeat.heldShards = heldShards
}

func (consumerHeatBeat *ConsumerHeatBeat) setHeartShards(heartShards []int) {
	shardLock.Lock()
	defer shardLock.Unlock()
	consumerHeatBeat.heartShards = heartShards
}

func (consumerHeatBeat *ConsumerHeatBeat) getHeartShards() []int {
	shardLock.RLock()
	defer shardLock.RUnlock()
	return consumerHeatBeat.heartShards
}

func (consumerHeatBeat *ConsumerHeatBeat) shutDownHeart() {
	level.Info(consumerHeatBeat.logger).Log("msg", "try to stop heart beat")
	consumerHeatBeat.shutDownFlag.Store(true)
}

func (consumerHeatBeat *ConsumerHeatBeat) heartBeatRun() {
	var lastHeartBeatTime int64

	for !consumerHeatBeat.shutDownFlag.Load() {
		lastHeartBeatTime = time.Now().Unix()
		uploadShards := append(consumerHeatBeat.heartShards, consumerHeatBeat.heldShards...)
		consumerHeatBeat.setHeartShards(Set(uploadShards))
		responseShards, err := consumerHeatBeat.client.heartBeat(consumerHeatBeat.getHeartShards())
		if err != nil {
			level.Warn(consumerHeatBeat.logger).Log("msg", "send heartbeat error", "error", err)
			if strings.Contains(err.Error(), "ConsumerGroupNotExist") {
				level.Error(consumerHeatBeat.logger).Log("msg", "send heartbeat and heartshutDown", "error", "ConsumerGroupNotExist")
				consumerHeatBeat.shutDownFlag.Store(true)
				consumerHeatBeat.Flag = 1
				//
			}
			if time.Now().Unix()-consumerHeatBeat.lastHeartBeatSuccessTime > int64(consumerHeatBeat.client.consumerGroup.Timeout+consumerHeatBeat.client.option.HeartbeatIntervalInSecond) {
				consumerHeatBeat.setHeldShards([]int{})
				level.Info(consumerHeatBeat.logger).Log("msg", "Heart beat timeout, automatic reset consumer held shards")
			}
		} else {
			consumerHeatBeat.lastHeartBeatSuccessTime = time.Now().Unix()
			level.Info(consumerHeatBeat.logger).Log("heart beat result", fmt.Sprintf("%v", consumerHeatBeat.heartShards), "get", fmt.Sprintf("%v", responseShards))
			consumerHeatBeat.setHeldShards(responseShards)
			if !IntSliceReflectEqual(consumerHeatBeat.getHeartShards(), consumerHeatBeat.getHeldShards()) {
				currentSet := Set(consumerHeatBeat.getHeartShards())
				responseSet := Set(consumerHeatBeat.getHeldShards())
				add := Subtract(currentSet, responseSet)
				remove := Subtract(responseSet, currentSet)
				level.Info(consumerHeatBeat.logger).Log("shard reorganize, adding:", fmt.Sprintf("%v", add), "removing:", fmt.Sprintf("%v", remove))
			}

		}
		TimeToSleepInSecond(int64(consumerHeatBeat.client.option.HeartbeatIntervalInSecond), lastHeartBeatTime, consumerHeatBeat.shutDownFlag.Load())
	}
	level.Info(consumerHeatBeat.logger).Log("msg", "heart beat exit")
}

func (consumerHeatBeat *ConsumerHeatBeat) removeHeartShard(shardId int) bool {
	shardLock.Lock()
	defer shardLock.Unlock()
	isDeleteShard := false
	for i, heartShard := range consumerHeatBeat.heartShards {
		if shardId == heartShard {
			consumerHeatBeat.heartShards = append(consumerHeatBeat.heartShards[:i], consumerHeatBeat.heartShards[i+1:]...)
			isDeleteShard = true
			break
		}
	}
	return isDeleteShard
}
