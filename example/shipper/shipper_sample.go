package main

import (
	"fmt"

	sls "github.com/sanallen/aliyun-log-go-sdk"
	"github.com/sanallen/aliyun-log-go-sdk/example/util"
)

func main() {
	fmt.Println("loghub shipper sample begin")
	logstoreName := util.LogStoreName
	logProject, err := sls.NewLogProject(util.ProjectName, util.Endpoint, util.AccessKeyID, util.AccessKeySecret)
	logstore, err := logProject.GetLogStore(logstoreName)
	if logstore == nil {
		fmt.Printf("GetLogStore fail, err:%v\n", err)
		err = logProject.CreateLogStore(logstoreName, 1, 2, true, 5)
		if err != nil {
			panic(err)
		}
		fmt.Println("CreateLogStore success")
	} else {
		fmt.Printf("GetLogStore success, name: %s, ttl: %d, shardCount: %d, createTime: %d, lastModifyTime: %d\n", logstore.Name, logstore.TTL, logstore.ShardCount, logstore.CreateTime, logstore.LastModifyTime)
	}

	ossShipperName := "ossshippertest"
	sc, err := logstore.GetShipper(ossShipperName)
	if err != nil {
		fmt.Println(err)
	}
	if sc == nil {
		ossShipperConfig := &sls.OSSShipperConfig{
			OssBucket:      "testBucket1",
			OssPrefix:      "testPrefix",
			RoleArn:        "acs:ram::account-id:role/log-to-oss", // replace with real role
			BufferInterval: 300,
			BufferSize:     100,
			CompressType:   "none",
			PathFormat:     "%Y/%m/%d/%H/%M",
			Format:         "json",
		}
		ossShipper := &sls.Shipper{
			ShipperName:         ossShipperName,
			TargetType:          sls.OSSShipperType,
			TargetConfiguration: ossShipperConfig,
		}
		err = logstore.CreateShipper(ossShipper)
		if err != nil {
			panic(err)
		}
		fmt.Println("CreateShipper success")
		sc, err = logstore.GetShipper(ossShipperName)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("GetShipper success %+v\n", sc.TargetConfiguration)
	if err := logstore.DeleteShipper(ossShipperName); err != nil {
		fmt.Println(err)
	}
	fmt.Println("DeleteShipper success")
}
