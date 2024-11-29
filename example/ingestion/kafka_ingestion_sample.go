package main

import (
	"encoding/json"
	"fmt"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
)

func main() {
	fmt.Println("create kafka ingestion sample begin")
	logstoreName := util.LogStoreName
	project := util.ProjectName
	client := util.Client
	base := sls.BaseJob{
		Name:        "ingest-kafka-test-kafka", // TODO
		DisplayName: "test-kafka",              // TODO
		Description: "test-kafka",              // TODO
		Type:        "Ingestion",               // default
	}
	sj := sls.ScheduledJob{
		BaseJob: base,
		Schedule: &sls.Schedule{
			Type: "Resident", // default
		},
	}
	kafkaSource := sls.KafkaSource{
		DataSource:       sls.DataSource{DataSourceType: sls.DataSourceKafka},
		Topics:           "test",                 // TODO test,test1
		BootStrapServers: "123.123.123.123:9092", // TODO
		ValueType:        "json",                 // TODO
		FromPosition:     "lastest",              // TODO
		Communication:    "{\"protocol\":\"sasl_ssl\",\"sasl\":{\"password\":\"yyy\",\"mechanism\":\"plain\",\"username\":\"xxx\"}}",
		NameResolutions:  "{\"localhost\":\"127.0.0.1\"}",
		VpcId:            "vpc-asdasdas",
	}
	source_tmp, _ := json.Marshal(&kafkaSource)
	var source map[string]interface{}
	_ = json.Unmarshal(source_tmp, &source)

	for k, v := range source {
		if v == nil {
			delete(source, k)
		}
	}

	ingestion := &sls.Ingestion{
		ScheduledJob: sj,
		IngestionConfiguration: &sls.IngestionConfiguration{
			Version:          "v2.0",
			LogStore:         logstoreName,
			NumberOfInstance: 0,
			DataSource:       source,
		},
	}
	if err := client.CreateIngestion(project, ingestion); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("create kafka ingestion over")
	}

}
