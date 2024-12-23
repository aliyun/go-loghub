package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
)

// variables you should fill
//
// sample processor spl:
// * | parse-regexp content, '(\S+)\s-\s(\S+)\s\[(\S+)\]\s"(\S+)\s(\S+)\s(\S+)"\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\S+)\s(\S+)\s"(.*)"' as remote_addr, remote_user, time_local, request_method, request_uri, http_protocol, request_time, request_length, status, body_bytes_sent, host, referer, user_agent | project-away content
var (
	accessKeyId     = os.Getenv("ACCESS_KEY_ID")
	accessKeySecret = os.Getenv("ACCESS_KEY_SECRET")
	endpoint        = ""
	project         = ""
	logstore        = ""
	processor       = ""
)

// mock data config
var (
	remoteUsers    = []string{"Alice", "Bob", "Candy", "David", "Elisa"}
	requestMethods = []string{"GET", "POST", "PUT", "DELETE", "HEAD"}
	statuses       = []string{"200", "301", "302", "400", "401", "403", "500", "501", "502"}
	httpProtocol   = "HTTP/1.1"
	userAgent      = "Mozilla/5.0 (Windows NT 5.2; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.41 Safari/535.1"
)

func mockNginxLog() string {
	var (
		remoteAddr    = fmt.Sprintf("192.168.1.%d", rand.Intn(100))
		remoteUser    = remoteUsers[rand.Intn(len(remoteUsers))]
		timeLocal     = time.Now().Format(time.RFC3339)
		requestMethod = requestMethods[rand.Intn(len(requestMethods))]
		requestUri    = fmt.Sprintf("/request/path-%d/file-%d", rand.Intn(10), rand.Intn(10))
		requestTime   = strconv.Itoa(rand.Intn(1000))
		requestLength = strconv.Itoa(rand.Intn(100000))
		status        = statuses[rand.Intn(len(statuses))]
		bodyBytesSent = strconv.Itoa(rand.Intn(100000))
		host          = fmt.Sprintf("www.test%d.com", rand.Intn(10))
		referer       = fmt.Sprintf("www.test%d.com", rand.Intn(10))
	)

	content := fmt.Sprintf(
		`%s - %s [%s] "%s %s %s" %s %s %s %s %s %s "%s"`,
		remoteAddr,
		remoteUser,
		timeLocal,
		requestMethod,
		requestUri,
		httpProtocol,
		requestTime,
		requestLength,
		status,
		bodyBytesSent,
		host,
		referer,
		userAgent,
	)
	return content
}

func mockLogGroup(lines int, topic string) *sls.LogGroup {
	logs := []*sls.Log{}
	for i := 0; i < lines; i++ {
		log := &sls.Log{
			Time: proto.Uint32(uint32(time.Now().Unix())),
			Contents: []*sls.LogContent{
				&sls.LogContent{
					Key:   proto.String("content"),
					Value: proto.String(mockNginxLog()),
				},
			},
		}
		logs = append(logs, log)
	}
	return &sls.LogGroup{
		Topic: proto.String(topic),
		Logs:  logs,
	}
}

func main() {
	client := sls.CreateNormalInterface(endpoint, accessKeyId, accessKeySecret, "")

	for {
		req := &sls.PostLogStoreLogsRequest{
			LogGroup:  mockLogGroup(10, "PostLogStoreLogsV2"),
			Processor: processor,
		}
		err := client.PostLogStoreLogsV2(project, logstore, req)
		fmt.Println(time.Now(), "PostLogStoreLogsV2", err)

		time.Sleep(time.Second)
	}
}
