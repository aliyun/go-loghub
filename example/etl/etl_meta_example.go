package main

import (
	"fmt"
	"strings"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
)

func main() {

	fmt.Println("etl_meta example begin")
	sls.GlobalForceUsingHTTP = true
	metaKey := "test-meta-key"
	createMeta := &sls.EtlMeta{
		MetaName: "xx-log",
		MetaKey:  metaKey,
		MetaTag:  "123456",
		MetaValue: map[string]string{
			"aliuid":   "123456",
			"region":   "cn-shenzhen",
			"project":  "test-project",
			"logstore": "test-logstore",
			"roleArn":  "acs:ram::123456:role/aliyunlogarchiverole",
		},
	}
	err := util.Client.CreateEtlMeta(util.ProjectName, createMeta)
	if err != nil {
		fmt.Printf("CreateEtlMeta fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else {
		fmt.Printf("CreateEtlMeta success\n")
	}
	etlMeta, err := util.Client.GetEtlMeta(util.ProjectName, "xx-log", metaKey)
	if err != nil {
		fmt.Printf("GetEtlMeta fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else {
		if etlMeta != nil {
			fmt.Printf("GetEtlMeta success, metaName: %s, metaKey: %s, metaTag: %s, metaValue: %s\n",
				etlMeta.MetaName, etlMeta.MetaKey, etlMeta.MetaTag, etlMeta.MetaValue)
		} else {
			fmt.Printf("GetEtlMeta success, no meta hit")
		}
	}

	updateMeta := &sls.EtlMeta{
		MetaName: "xx-log",
		MetaKey:  metaKey,
		MetaTag:  "123456",
		MetaValue: map[string]string{
			"aliuid":   "123456",
			"region":   "cn-qingdao",
			"project":  "test-project-2",
			"logstore": "test-logstore-2",
			"roleArn":  "acs:ram::123456:role/aliyunlogarchiverole",
		},
	}
	err = util.Client.UpdateEtlMeta(util.ProjectName, updateMeta)
	if err != nil {
		fmt.Printf("UpdateEtlMeta fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else {
		fmt.Printf("UpdateEtlMeta success\n")
	}
	etlMeta, err = util.Client.GetEtlMeta(util.ProjectName, "xx-log", metaKey)
	if err != nil {
		fmt.Printf("GetEtlMeta fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else {
		if etlMeta != nil {
			fmt.Printf("GetEtlMeta success, metaName: %s, metaKey: %s, metaTag: %s, metaValue: %s\n",
				etlMeta.MetaName, etlMeta.MetaKey, etlMeta.MetaTag, etlMeta.MetaValue)
		} else {
			fmt.Printf("GetEtlMeta success, no meta hit")
		}
	}

	err = util.Client.DeleteEtlMeta(util.ProjectName, "xx-log", metaKey)
	if err != nil {
		fmt.Printf("DeletEtlMeta fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else {
		fmt.Printf("DeleteEtlMeta success\n")
	}
	etlMeta, err = util.Client.GetEtlMeta(util.ProjectName, "xx-log", metaKey)
	if err != nil {
		fmt.Printf("GetEtlMeta fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else {
		if etlMeta != nil {
			fmt.Printf("GetEtlMeta success, metaName: %s, metaKey: %s, metaTag: %s, metaValue: %s\n",
				etlMeta.MetaName, etlMeta.MetaKey, etlMeta.MetaTag, etlMeta.MetaValue)
		} else {
			fmt.Printf("GetEtlMeta success, no meta hit")
		}
	}

	total, count, etlMetaNameList, err := util.Client.ListEtlMetaName(util.ProjectName, 0, 100)
	if err != nil {
		fmt.Printf("ListEtlMetaName fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else if count > 0 {
		fmt.Printf("ListEtlMetaName success, total: %d, count: %d\n", total, len(etlMetaNameList))
		for index, value := range etlMetaNameList {
			fmt.Printf("index: %d, metaName: %s\n", index, value)
		}
	}

	total, count, etlMetaList, err := util.Client.ListEtlMeta(util.ProjectName, "xx-log", 0, 100)
	if err != nil {
		fmt.Printf("ListEtlMeta fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else if count > 0 {
		fmt.Printf("ListEtlMeta success, total: %d, count: %d\n", total, len(etlMetaList))
		for index, value := range etlMetaList {
			fmt.Printf("index: %d, metaName: %s, metaKey: %s, metaTag: %s, metaValue:%s\n",
				index, value.MetaName, value.MetaKey, value.MetaTag, value.MetaValue)
		}
	}

	total, count, etlMetaList, err = util.Client.ListEtlMetaWithTag(util.ProjectName, "xx-log", "123456", 0, 100)
	if err != nil {
		fmt.Printf("ListEtlMetaWithTag fail, err:%v\n", err)
		if strings.Contains(err.Error(), sls.POST_BODY_INVALID) {
			return
		}
	} else if count > 0 {
		fmt.Printf("ListEtlMetaWithTag success, total: %d, count: %d\n", total, len(etlMetaList))
		for index, value := range etlMetaList {
			fmt.Printf("index: %d, metaName: %s, metaKey: %s, metaTag: %s, metaValue:%s\n",
				index, value.MetaName, value.MetaKey, value.MetaTag, value.MetaValue)
		}
	}

	fmt.Println("etl_meta example end")
}
