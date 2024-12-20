package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/aliyun/aliyun-log-go-sdk/producer"
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

func main() {
	config := producer.GetDefaultProducerConfig()
	config.Endpoint = endpoint
	config.AccessKeyID = accessKeyId
	config.AccessKeySecret = accessKeySecret
	config.GeneratePackId = true
	config.Processor = processor

	producerInstance, err := producer.NewProducer(config)
	if err != nil {
		panic(err)
	}
	producerInstance.Start()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				log := producer.GenerateLog(
					uint32(time.Now().Unix()),
					map[string]string{"content": mockNginxLog()},
				)
				err := producerInstance.SendLog(project, logstore, "producer", "", log)
				if err != nil {
					fmt.Println(err)
				}
			}
		}()
	}
	wg.Wait()
	fmt.Println("Send completion")

	term := make(chan os.Signal)
	signal.Notify(term, os.Kill, os.Interrupt)
	if _, ok := <-term; ok {
		fmt.Println("Get the shutdown signal and start to shut down")
		producerInstance.Close(60000)
	}
}
