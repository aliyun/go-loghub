package consumerLibrary

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/atomic"
)

type ConsumerHeartBeat struct {
	client                   *ConsumerClient
	shutDownFlag             *atomic.Bool
	heldShards               []int
	heartShards              []int
	logger                   log.Logger
	lastHeartBeatSuccessTime int64
	shardLock                sync.RWMutex
}

func initConsumerHeatBeat(consumerClient *ConsumerClient, logger log.Logger) *ConsumerHeartBeat {
	consumerHeartBeat := &ConsumerHeartBeat{
		client:                   consumerClient,
		shutDownFlag:             atomic.NewBool(false),
		heldShards:               []int{},
		heartShards:              []int{},
		logger:                   logger,
		lastHeartBeatSuccessTime: time.Now().Unix(),
	}
	return consumerHeartBeat
}

func (heartbeat *ConsumerHeartBeat) getHeldShards() []int {
	heartbeat.shardLock.RLock()
	defer heartbeat.shardLock.RUnlock()
	return append([]int{}, heartbeat.heldShards...)
}

func (heartbeat *ConsumerHeartBeat) setHeldShards(heldShards []int) {
	heartbeat.shardLock.Lock()
	defer heartbeat.shardLock.Unlock()
	heartbeat.heldShards = heldShards
}

func (heartbeat *ConsumerHeartBeat) getHeartShards() []int {
	heartbeat.shardLock.RLock()
	defer heartbeat.shardLock.RUnlock()
	return append([]int{}, heartbeat.heartShards...)
}

func (heartbeat *ConsumerHeartBeat) shutDownHeart() {
	level.Info(heartbeat.logger).Log("msg", "try to stop heart beat")
	heartbeat.shutDownFlag.Store(true)
}

func (heartbeat *ConsumerHeartBeat) updateHeartShard() {
	m := make(map[int]bool)
	heartbeat.shardLock.Lock()
	defer heartbeat.shardLock.Unlock()
	for _, shard := range heartbeat.heartShards {
		m[shard] = true
	}
	for _, shard := range heartbeat.heldShards {
		m[shard] = true
	}

	uploadShards := make([]int, 0, len(m))
	for shard := range m {
		uploadShards = append(uploadShards, shard)
	}
	heartbeat.heartShards = uploadShards
}

func (heartbeat *ConsumerHeartBeat) heartBeatRun() {
	var lastHeartBeatTime int64

	for !heartbeat.shutDownFlag.Load() {
		lastHeartBeatTime = time.Now().Unix()
		heartbeat.updateHeartShard()
		responseShards, err := heartbeat.client.heartBeat(heartbeat.getHeartShards())
		if err != nil {
			level.Warn(heartbeat.logger).Log("msg", "send heartbeat error", "error", err)
			if time.Now().Unix()-heartbeat.lastHeartBeatSuccessTime > int64(heartbeat.client.consumerGroup.Timeout+heartbeat.client.option.HeartbeatIntervalInSecond) {
				heartbeat.setHeldShards([]int{})
				level.Info(heartbeat.logger).Log("msg", "Heart beat timeout, automatic reset consumer held shards")
			}
		} else {
			heartbeat.lastHeartBeatSuccessTime = time.Now().Unix()
			level.Info(heartbeat.logger).Log("heart beat result", fmt.Sprintf("%v", heartbeat.heartShards), "get", fmt.Sprintf("%v", responseShards))
			heartbeat.setHeldShards(responseShards)
			if !IntSliceReflectEqual(heartbeat.getHeartShards(), heartbeat.getHeldShards()) {
				currentSet := Set(heartbeat.getHeartShards())
				responseSet := Set(heartbeat.getHeldShards())
				add := Subtract(currentSet, responseSet)
				remove := Subtract(responseSet, currentSet)
				level.Info(heartbeat.logger).Log("shard reorganize, adding:", fmt.Sprintf("%v", add), "removing:", fmt.Sprintf("%v", remove))
			}

		}
		TimeToSleepInSecond(int64(heartbeat.client.option.HeartbeatIntervalInSecond), lastHeartBeatTime, heartbeat.shutDownFlag.Load())
	}
	level.Info(heartbeat.logger).Log("msg", "heart beat exit")
}

func (heartbeat *ConsumerHeartBeat) removeHeartShard(shardId int) bool {
	heartbeat.shardLock.Lock()
	defer heartbeat.shardLock.Unlock()
	isDeleteShard := false
	for i, heartShard := range heartbeat.heartShards {
		if shardId == heartShard {
			heartbeat.heartShards = append(heartbeat.heartShards[:i], heartbeat.heartShards[i+1:]...)
			isDeleteShard = true
			break
		}
	}
	return isDeleteShard
}
