package main

import (
	"encoding/json"
	"fmt"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"time"
)

const (
	endpoint        = "your endpoint" // https://help.aliyun.com/document_detail/29008.html
	accessKeyId     = "your akId"
	accessKeySecret = "your akSecret"
	securityToken   = ""
	projectName     = "your project name"
	logStore        = "your logstore name"
	roleArn         = "your roleArn"
	jobName         = "your job name"
	bucket          = "your bucket"
	compressType    = sls.OSSCompressionTypeNone
	contentType     = sls.OSSContentDetailTypeORC
)

func main() {
	// create the client with ak and endpoint
	client := sls.CreateNormalInterface(endpoint, accessKeyId, accessKeySecret, securityToken)
	// create the oss sink export job
	if err := client.CreateExport(projectName, getOssExport(contentType, compressType)); err != nil {
		fmt.Println(err)
	}
	// get the export job
	if getExport, err := client.GetExport(projectName, jobName); err != nil {
		fmt.Println(err)
	} else {
		detail, _ := json.Marshal(getExport)
		fmt.Println(string(detail))
	}
	// list the jobs under the logStore
	if exports, total, count, err := client.ListExport(projectName, logStore, "", "", 0, 10); err != nil {
		fmt.Println(err)
	} else {
		detail, _ := json.Marshal(exports)
		fmt.Println(string(detail))
		fmt.Println(total)
		fmt.Println(count)
	}
	//client.UpdateExport(projectName, getOssExport(contentType, compressType))
	//client.DeleteExport(projectName, jobName)
}

func getOssExport(contentType sls.OSSContentType, compressionType sls.OSSCompressionType) *sls.Export {
	timeUnix := time.Now().Unix()
	return &sls.Export{
		ScheduledJob: sls.ScheduledJob{
			BaseJob: sls.BaseJob{
				Name:        jobName,
				DisplayName: jobName,
				Description: "",
				Type:        sls.EXPORT_JOB,
			},
			Schedule: &sls.Schedule{
				Type: "Resident",
			},
		},
		ExportConfiguration: &sls.ExportConfiguration{
			FromTime:   timeUnix - 3600,
			ToTime:     0,
			LogStore:   logStore,
			Parameters: make(map[string]string),
			RoleArn:    roleArn,
			Version:    sls.ExportVersion2,
			DataSink: &sls.AliyunOSSSink{
				Type:            sls.DataSinkOSS,
				RoleArn:         roleArn,
				Bucket:          bucket,
				Prefix:          "",
				Suffix:          "",
				PathFormat:      "%Y/%m/%d/%H/%M",
				PathFormatType:  "time",
				BufferSize:      256,
				BufferInterval:  300,
				TimeZone:        "+0800",
				ContentType:     contentType,
				CompressionType: compressionType,
				ContentDetail:   getContentDetail(contentType),
			},
		},
	}
}

func getContentDetail(contentType sls.OSSContentType) interface{} {
	if contentType == sls.OSSContentDetailTypeCSV {
		// default csvDetail ,you can replace the parameters
		csvDetail := sls.CsvContentDetail{
			ColumnNames: append(make([]string, 0), "k1", "k2"), // column key
			Delimiter:   ",",                                   // you can set " " , "|" , "," and "\t"
			Quote:       "\"",                                  // you can set "'" （single quote）  , "\"" （double quote） and ""
			Escape:      "\"",
			Null:        "",
			Header:      true,
			LineFeed:    "\n",
		}
		return csvDetail
	}

	if contentType == sls.OSSContentDetailTypeJSON {
		// default enableTag
		jsonDetail := sls.JsonContentDetail{
			EnableTag: true,
		}
		return jsonDetail
	}
	if contentType == "parquet" {
		parquetDetail := sls.ParquetContentDetail{
			Columns: []sls.Column{
				{Name: "newline", Type: "string"},
				{Name: "chinese", Type: "string"},
				{Name: "special characters", Type: "string"},
				{Name: "normal", Type: "string"},
				{Name: "user_escape", Type: "string"},
				{Name: "int32_field", Type: "int32"},
				{Name: "int64_field", Type: "int64"},
				{Name: "boolean_field", Type: "boolean"},
				{Name: "float_field", Type: "float"},
				{Name: "double_field", Type: "double"},
			},
		}
		return parquetDetail
	}
	if contentType == "orc" {
		orcDetail := sls.OrcContentDetail{
			Columns: []sls.Column{
				{Name: "newline", Type: "string"},
				{Name: "chinese", Type: "string"},
				{Name: "special characters", Type: "string"},
				{Name: "normal", Type: "string"},
				{Name: "user_escape", Type: "string"},
				{Name: "int32_field", Type: "int32"},
				{Name: "int64_field", Type: "int64"},
				{Name: "boolean_field", Type: "boolean"},
				{Name: "float_field", Type: "float"},
				{Name: "double_field", Type: "double"},
			},
		}
		return orcDetail
	}
	return nil
}
