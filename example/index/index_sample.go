package main

import (
	"fmt"
	"math/rand"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
	"github.com/gogo/protobuf/proto"
)

func main() {

	fmt.Println("loghub sample begin")
	logstore_name := "test"
	util.Client.DeleteLogStore(util.ProjectName, logstore_name)
	time.Sleep(15 * 1000 * time.Millisecond)
	err := util.Client.CreateLogStore(util.ProjectName, logstore_name, 1, 2, true, 16)
	if err != nil {
		fmt.Printf("CreateLogStore fail, err: %s", err)
		return
	}
	time.Sleep(15 * 1000 * time.Millisecond)
	fmt.Println("CreateLogStore success")
	logstore, err := util.Client.GetLogStore(util.ProjectName, logstore_name)
	if err != nil {
		fmt.Printf("GetLogStore fail, err: %s", err)
		return
	}
	fmt.Printf("GetLogStore success, name: %s, ttl: %d, shardCount: %d, createTime: %d, lastModifyTime: %d\n", logstore.Name, logstore.TTL, logstore.ShardCount, logstore.CreateTime, logstore.LastModifyTime)
	indexKeys := map[string]sls.IndexKey{
		"col_0": sls.IndexKey{
			Token:         []string{" "},
			CaseSensitive: false,
			Type:          "long",
		},
		"col_1": sls.IndexKey{
			Token:         []string{",", ":", " "},
			CaseSensitive: false,
			Type:          "text",
		},
	}
	index := sls.Index{
		Keys: indexKeys,
		Line: &sls.IndexLine{
			Token:         []string{",", ":", " "},
			CaseSensitive: false,
			IncludeKeys:   []string{},
			ExcludeKeys:   []string{},
		},
	}
	err = util.Client.CreateIndex(util.ProjectName, logstore_name, index)
	if err != nil {
		fmt.Printf("CreateIndex fail, err: %s", err)
		return
	}
	fmt.Println("CreateIndex success")
	time.Sleep(30 * 1000 * time.Millisecond)
	begin_time := uint32(time.Now().Unix())
	rand.Seed(int64(begin_time))
	// put logs to logstore
	for loggroupIdx := 0; loggroupIdx < 10; loggroupIdx++ {
		logs := []*sls.Log{}
		for logIdx := 0; logIdx < 100; logIdx++ {
			content := []*sls.LogContent{}
			for colIdx := 0; colIdx < 10; colIdx++ {
				if colIdx == 0 {
					content = append(content, &sls.LogContent{
						Key:   proto.String(fmt.Sprintf("col_%d", colIdx)),
						Value: proto.String(fmt.Sprintf("%d", rand.Intn(10000000))),
					})
				} else {
					content = append(content, &sls.LogContent{
						Key:   proto.String(fmt.Sprintf("col_%d", colIdx)),
						Value: proto.String(fmt.Sprintf("loggroup idx: %d, log idx: %d, col idx: %d, value: %d", loggroupIdx, logIdx, colIdx, rand.Intn(10000000))),
					})
				}
			}
			log := &sls.Log{
				Time:     proto.Uint32(uint32(time.Now().Unix())),
				Contents: content,
			}
			logs = append(logs, log)
		}
		loggroup := &sls.LogGroup{
			Topic:  proto.String(""),
			Source: proto.String("10.230.201.117"),
			Logs:   logs,
		}
		// PutLogs API Ref: https://intl.aliyun.com/help/doc-detail/29026.htm
		err = util.Client.PutLogs(util.ProjectName, logstore_name, loggroup)
		if err == nil {
			fmt.Println("PutLogs success")
		} else {
			fmt.Printf("PutLogs fail, err: %s\n", err)
		}
		time.Sleep(1000 * time.Millisecond)
	}
	end_time := uint32(time.Now().Unix())
	time.Sleep(15 * 1000 * time.Millisecond)
	// search logs from index on logstore
	totalCount := int64(0)
	for {
		// GetHistograms API Ref: https://intl.aliyun.com/help/doc-detail/29030.htm
		ghResp, err := util.Client.GetHistograms(util.ProjectName, logstore_name, "", int64(begin_time), int64(end_time), "col_0 > 1000000")
		if err != nil {
			fmt.Printf("GetHistograms fail, err: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		fmt.Printf("complete: %s, count: %d, histograms: %v\n", ghResp.Progress, ghResp.Count, ghResp.Histograms)
		totalCount += ghResp.Count
		if ghResp.Progress == "Complete" {
			break
		}
	}
	offset := int64(0)
	// get logs repeatedly with (offset, lines) parameters to get complete result
	for offset < totalCount {
		// GetLogs API Ref: https://intl.aliyun.com/help/doc-detail/29029.htm
		glResp, err := util.Client.GetLogs(util.ProjectName, logstore_name, "", int64(begin_time), int64(end_time), "col_0 > 1000000", 100, offset, false)
		if err != nil {
			fmt.Printf("GetLogs fail, err: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		fmt.Printf("Progress:%s, Count:%d, offset: %d\n", glResp.Progress, glResp.Count, offset)
		offset += glResp.Count
		if glResp.Count > 0 {
			fmt.Printf("logs: %v\n", glResp.Logs)
		}
		if glResp.Progress == "Complete" && glResp.Count == 0 {
			break
		}
	}
	fmt.Println("index sample end")
}
