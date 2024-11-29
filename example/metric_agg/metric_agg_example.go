package main

import (
	"encoding/json"
	"fmt"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

func crud(client *sls.Client, sourceProject string, aggRules *sls.MetricAggRules, testId string) {
	err := client.CreateMetricAggRules(sourceProject, aggRules)
	if err != nil {
		panic(err)
	}

	listAggRules, err := client.ListMetricAggRules(sourceProject, 0, 10)
	if err != nil {
		panic(err)
	}
	listAggRulesJson, _ := json.Marshal(listAggRules)
	fmt.Println(string(listAggRulesJson))

	err = client.UpdateMetricAggRules(sourceProject, aggRules)
	if err != nil {
		panic(err)
	}

	newAggRules, err := client.GetMetricAggRules(sourceProject, testId)
	if err != nil {
		panic(err)
	}
	newAggRulesJson, _ := json.Marshal(newAggRules)
	fmt.Println(string(newAggRulesJson))

	err = client.DeleteMetricAggRules(sourceProject, testId)
	if err != nil {
		panic(err)
	}
}

func sqlConfig(accessKeyID string, accessKeySecret string, testId string) *sls.MetricAggRules {
	aggRuleItem := &sls.MetricAggRuleItem{
		Name:      testId,
		QueryType: sls.MetricAggRulesSQL,
		Query:     "* | select max(__time__) as time, COUNT_if(Status < 500) as success, count_if(Status >= 500) as fail, count(1) as total, InvokerUid as aliuid, Project as project, LogStore as logstore from log  group by InvokerUid, Project, LogStore limit 100000",
		TimeName:  "time",
		MetricNames: []string{
			"success",
			"fail",
			"total",
		},
		LabelNames: map[string]string{
			"aliuid":   "aliuid",
			"logstore": "logstore",
			"project":  "project",
		},
		BeginUnixTime: 1610506297,
		EndUnixTime:   -1,
		Interval:      30,
		DelaySeconds:  30,
	}
	aggRuleItem1 := &sls.MetricAggRuleItem{
		Name:      "testId2",
		QueryType: sls.MetricAggRulesSQL,
		Query:     "* | select max(__time__) as time, COUNT_if(Status < 300) as ok, count_if(Status >= 300) as not_ok, Method as method,UserAgent as agent from log  group by method, agent limit 100000",
		TimeName:  "time",
		MetricNames: []string{
			"ok",
			"not_ok",
		},
		LabelNames: map[string]string{
			"method": "method",
			"agent":  "agent",
		},
		BeginUnixTime: 1610506297,
		EndUnixTime:   -1,
		Interval:      30,
		DelaySeconds:  30,
	}
	aggRules := &sls.MetricAggRules{
		ID:                  testId,
		Name:                testId,
		Desc:                "测试CreateMetricAggRules",
		SrcStore:            "internal-operation_log",
		SrcAccessKeyID:      accessKeyID,
		SrcAccessKeySecret:  accessKeySecret,
		DestEndpoint:        "cn-hangzhou-intranet.log.aliyuncs.com",
		DestProject:         "test-hangzhou-b",
		DestStore:           "test",
		DestAccessKeyID:     accessKeyID,
		DestAccessKeySecret: accessKeySecret,
		AggRules:            []sls.MetricAggRuleItem{*aggRuleItem, *aggRuleItem1},
	}
	return aggRules
}

func promqlConfig(accessKeyID string, accessKeySecret string, testId string) *sls.MetricAggRules {
	aggRuleItem := &sls.MetricAggRuleItem{
		Name:      testId,
		QueryType: sls.MetricAggRulesPromQL,
		Query:     "* | SELECT promql_query('sum(sum_over_time(total[1m]))') FROM  metrics limit 1000",
		TimeName:  "time",
		MetricNames: []string{
			"total_count",
		},
		LabelNames:    map[string]string{},
		BeginUnixTime: 1610433565,
		EndUnixTime:   -1,
		Interval:      60,
		DelaySeconds:  60,
	}
	aggRules := &sls.MetricAggRules{
		ID:                  testId,
		Name:                testId,
		Desc:                "测试CreateMetricAggRules",
		SrcStore:            "test",
		SrcAccessKeyID:      accessKeyID,
		SrcAccessKeySecret:  accessKeySecret,
		DestEndpoint:        "cn-hangzhou-intranet.log.aliyuncs.com",
		DestProject:         "test-hangzhou-b",
		DestStore:           "test2",
		DestAccessKeyID:     accessKeyID,
		DestAccessKeySecret: accessKeySecret,
		AggRules:            []sls.MetricAggRuleItem{*aggRuleItem},
	}
	return aggRules
}

func main() {
	accessKeyID := ""
	accessKeySecret := ""
	sourceProject := "k8s-log-cdc990939f2f547e883a4cb9236e85872"
	client := &sls.Client{
		Endpoint:        "cn-hangzhou.log.aliyuncs.com",
		AccessKeyID:     accessKeyID,
		AccessKeySecret: accessKeySecret,
	}

	testId := "metric_agg_rules1"
	aggRules := sqlConfig(accessKeyID, accessKeySecret, testId)
	crud(client, sourceProject, aggRules, testId)

	testId = "metric_agg_rules2"
	aggRules = promqlConfig(accessKeyID, accessKeySecret, testId)
	crud(client, sourceProject, aggRules, testId)
}
