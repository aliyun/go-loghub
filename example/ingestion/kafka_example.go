package main

import (
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/aliyun/aliyun-log-go-sdk"
)

const (
	endpoint        = "your_sls_endpoint"
	accessKeyId     = "your_ak_id"
	accessKeySecret = "your_ak_secret"
)

const (
	project  = "your_project_name"
	logstore = "your_logstore_name"
)

func get_sample_job_config(job_name string) map[string]interface{} {
	job := map[string]interface{}{
		"name":        job_name,
		"displayName": job_name,
		"type":        "Ingestion",
		"description": "",
		"state":       "Enabled",
		"schedule": map[string]interface{}{
			"type":           "Resident",
			"runImmediately": true,
		},
		"configuration": map[string]interface{}{
			"logstore": logstore,
			"version":  "v2.0",
			"source": map[string]interface{}{
				"type":              "Kafka",
				"bootstrapServers":  "xxx.xxx.xx.xx:9092,xxx.xxx.xx.xx:9092", // 服务地址，多个用逗号分隔
				"topics":            "rds,sls,oss",                           // Topic列表，多个用逗号分隔，支持正则
				"consumerGroup":     "licheng-test",                          // 消费组（可选，访问阿里云Kafka时提供）
				"fromPosition":      "earliest",                              // 起始位置，取值earlies,latest
				"valueType":         "JSON",                                  // 数据格式，取值JSON,TEXT
				"encoding":          "UTF-8",                                 // 编码格式，取值UTF-8,GBK
				"vpcId":             "vpc-xxx",                               // VPC实例ID（可选，访问阿里云Kafka集群时可提供）
				"timeField":         "origin_time",                           // 时间字段（可选）
				"timePattern":       "xxx",                                   // 提取时间正则（可选）
				"timeFormat":        "epochMicro",                            // 时间字段格式（可选），如epoch,yyyy-MM-dd HH:mm:ss等
				"timeZone":          "UTC",                                   // 时间字段时区（可选）
				"defaultTimeSource": "kafka",                                 // 默认时间来源，取值kafka,system
				"enableSlsContext":  "True",                                  // 日志上下文，取值true,fale
				"communication":     "",                                      // 通信协议（可选）
				"nameResolutions":   "",                                      // 私网域名解析（可选）
			},
		},
	}

	return job
}

func create_job(job_name string, client sdk.ClientInterface) {
	if job_name == "" {
		job_name = fmt.Sprintf("ingest-kafka-%d-0516", time.Now().Unix())
	}

	job_config := get_sample_job_config(job_name)
	bytes, _ := json.Marshal(job_config)

	if err := client.CreateAlertString(project, string(bytes)); err != nil {
		panic(err)
	}
	fmt.Println("created job: " + job_name + ", definition: " + string(bytes))
}

func update_job(job_name string, client sdk.ClientInterface) {
	job_config := get_sample_job_config(job_name)
	bytes, _ := json.Marshal(job_config)

	if err := client.UpdateAlertString(project, job_name, string(bytes)); err != nil {
		panic(err)
	}
	fmt.Println("updated job: " + job_name + ", definition: " + string(bytes))
}

func get_job(job_name string, client sdk.ClientInterface) {
	job_string, err := client.GetAlertString(project, job_name)
	if err != nil {
		panic(err)
	}
	fmt.Println("got job: " + job_name + ", definition: " + job_string)
}

func delete_job(job_name string, client sdk.ClientInterface) {
	if err := client.DeleteIngestion(project, job_name); err != nil {
		panic(err)
	}
	fmt.Println("deleted job: " + job_name)
}

func restart_job(job_name string, client sdk.ClientInterface) {
	if err := client.StopETL(project, job_name); err != nil {
		panic(err)
	}
	fmt.Println("stopping job: " + job_name + "...")
	time.Sleep(10 * time.Second)

	retry_cnt := 0
	for {
		if err := client.StartETL(project, job_name); err != nil {
			retry_cnt += 1
			if retry_cnt > 10 {
				fmt.Println("restart job: " + job_name + "timeout.")
				break
			}
			time.Sleep(10 * time.Second)
		} else {
			fmt.Println("restart job: " + job_name + " successfully.")
			break
		}
	}
}

func main() {
	client := sdk.CreateNormalInterface(endpoint, accessKeyId, accessKeySecret, "")
	defer client.Close()

	create_job("", client)
	update_job("ingest-kafka-1684236554-0516", client) // 注意：update_job修改配置后需要再调用restart_job后修改才会生效
	restart_job("ingest-kafka-1684236554-0516", client)
	get_job("ingest-kafka-1684236554-0516", client)
	delete_job("ingest-kafka-1684236554-0516", client)
}
