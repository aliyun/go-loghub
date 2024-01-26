package main

import (
	"encoding/json"
	"fmt"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
)

func main() {
	fmt.Println("create s3 ingestion sample begin")
	logstoreName := util.LogStoreName
	project := util.ProjectName
	client := util.Client
	base := sls.BaseJob{
		Name:        "ingest-s3-test2",  // TODO
		DisplayName: "s3 bucket import", // TODO
		Description: "test-s3",          // TODO
		Type:        "Ingestion",        // default
	}
	sj := sls.ScheduledJob{
		BaseJob: base,
		Schedule: &sls.Schedule{
			Type: "Resident", // default
		},
	}

	s3Source := sls.S3Source{
		DataSource:         sls.DataSource{DataSourceType: sls.DataSourceS3},
		AWSAccessKey:       util.AWSAccessKey,
		AWSAccessKeySecret: util.AWSAccessKeySecret,
		AWSRegion:          "", // TODO
		Bucket:             "", // TODO
		Prefix:             "", // TODO
		Format: map[string]string{
			"type":     "json",
			"encoding": "UTF-8",
			"interval": "5m",
		},
		CompressionCodec: "none",
	}
	source_tmp, _ := json.Marshal(&s3Source)
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
		fmt.Println("create s3 ingestion over")
	}

}
