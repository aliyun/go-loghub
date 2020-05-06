package main

import (
	"fmt"
	"time"

	"github.com/sanallen/aliyun-log-go-sdk"
	"github.com/sanallen/aliyun-log-go-sdk/example/util"
)

func main() {
	err := util.Client.CreateLogStore(util.ProjectName,util.LogStoreName,2,2,true,64)
	if err != nil {
		panic(err)
	}
	logstore, err := util.Client.GetLogStore(util.ProjectName, util.LogStoreName)
	if err != nil {
		panic(err)
	}
	fmt.Println("create logstore successfully:", logstore.Name)

	updateLogstore := &sls.LogStore{
		Name:util.LogStoreName,
		TTL:2,
		ShardCount:10,
		AutoSplit:false,
		WebTracking:true,
	}
	err = util.Client.UpdateLogStoreV2(util.ProjectName,updateLogstore)
	if err != nil {
		panic(err)
	}
	fmt.Println("update logstore suecessed")
	fmt.Println("Prepare to delete the logstore after 30 seconds")
	time.Sleep(30 * time.Second)
	err = util.Client.DeleteLogStore(util.ProjectName, util.LogStoreName)
	if err != nil {
		panic(err)
	}
	fmt.Println("Delete Logstore successfully")
}
