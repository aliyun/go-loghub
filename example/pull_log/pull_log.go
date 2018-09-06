package main

import (
	"fmt"
	"strconv"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

func main() {
	sls.GlobalForceUsingHTTP = true
	client := sls.CreateNormalInterface("cn-hangzhou.log.aliyuncs.com", "access-key-id", "access-key-secret", "")
	project := "my-project"
	logstore := "my-logstore"

	shards, err := client.ListShards(project, logstore)
	if err != nil {
		panic(err)
	}

	fmt.Printf("[shards] %v \n", shards)

	now := time.Now()
	totalLogCount := 0
	for _, shard := range shards {
		fmt.Printf("[shard] %d begin\n", shard.ShardID)
		from := time.Date(now.Year(), now.Month(), 04, 19, 00, 0, 0, time.Local)
		end := time.Date(now.Year(), now.Month(), 04, 20, 00, 0, 0, time.Local)
		beginCursor, err := client.GetCursor(project, logstore, shard.ShardID, strconv.Itoa((int)(from.Unix())))
		if err != nil {
			panic(err)
		}
		endCursor, err := client.GetCursor(project, logstore, shard.ShardID, strconv.Itoa((int)(end.Unix())))
		if err != nil {
			panic(err)
		}

		nextCursor := beginCursor
		for nextCursor != endCursor {
			gl, nc, err := client.PullLogs(project, logstore, shard.ShardID, nextCursor, endCursor, 10)
			if err != nil {
				fmt.Printf("pull log error : %s\n", err)
				time.Sleep(time.Second)
				continue
			}
			nextCursor = nc
			fmt.Printf("now count %d \n", totalLogCount)
			if gl != nil {
				for _, lg := range gl.LogGroups {
					for _, tag := range lg.LogTags {
						fmt.Printf("[tag] %s : %s\n", tag.GetKey(), tag.GetValue())
					}
					for _, log := range lg.Logs {
						totalLogCount++
						// print log
						for _, content := range log.Contents {
							continue
							fmt.Printf("[log] %s : %s\n", content.GetKey(), content.GetValue())
						}
					}
				}
			}
		}
		fmt.Printf("[shard] %d done\n", shard.ShardID)
	}
	fmt.Printf("[total] %d \n", totalLogCount)
}
