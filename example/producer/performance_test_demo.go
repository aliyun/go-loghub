package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/producer"
	"github.com/gogo/protobuf/proto"
)

var valueList [][]*string

func main() {
	runtime.GOMAXPROCS(2)
	producerConfig := producer.GetDefaultProducerConfig()
	producerConfig.MaxBatchCount = 40960
	producerConfig.MaxBatchSize = 3 * 1024 * 1024
	producerConfig.Endpoint = os.Getenv("Endpoint")
	producerConfig.AccessKeyID = os.Getenv("AccessKeyID")
	producerConfig.AccessKeySecret = os.Getenv("AccessKeySecret")
	keys := getKeys()
	rand.Seed(time.Now().Unix())
	valueList = generateValuseList()

	producerInstance, err := producer.NewProducer(producerConfig)
	if err != nil {
		panic(err)
	}
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Kill, os.Interrupt)
	producerInstance.Start()
	fmt.Println("start send logs")
	for i := 0; i < 10; i++ {
		go func(id int) {
			for i := 0; i < 200000000; i++ {
				r := rand.Intn(200000000)
				err := producerInstance.SendLog("project", "logstore", generateTopic(r), generateSource(r), getLog(keys))
				if err != nil {
					fmt.Println(err)
					break
				}
			}
			fmt.Println("All data in the queue has been sent, goroutine id:", id)
		}(i)
	}
	if _, ok := <-ch; ok {
		fmt.Println("Get the shutdown signal and start to shut down")
		producerInstance.SafeClose()
	}

}

func generateTopic(r int) string {
	return fmt.Sprintf("topic-%v", r%5)
}

func generateSource(r int) string {
	return fmt.Sprintf("source-%v", r%10)
}

func getKeys() (keys []*string) {
	for i := 1; i < 9; i++ {
		key := proto.String(fmt.Sprintf("content_key_%v", i))
		keys = append(keys, key)
	}
	return keys
}

func getValues() (values []*string) {
	r := rand.Intn(20000000)
	for i := 1; i < 9; i++ {
		value := proto.String(fmt.Sprintf("%vabcdefghijklmnopqrstuvwxyz0123456789!@#$^&*()_012345678-%v", i, r))
		values = append(values, value)
	}
	return values
}

func getLog(keys []*string) *sls.Log {
	contents := []*sls.LogContent{}
	r := rand.Intn(4096)
	for i := 0; i < 8; i++ {
		content := &sls.LogContent{
			Key:   keys[i],
			Value: valueList[r][i],
		}
		contents = append(contents, content)
	}
	log := &sls.Log{
		Time:     proto.Uint32(uint32(time.Now().Unix())),
		Contents: contents,
	}
	return log
}

func generateValuseList() [][]*string {
	for i := 0; i < 4097; i++ {
		v := getValues()
		valueList = append(valueList, v)
	}
	return valueList
}
