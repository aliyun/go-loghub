package producer

import (
	"fmt"
	"os"
	"os/signal"
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
)

func TestV4Sign(t *testing.T) {
	producerConfig := GetDefaultProducerConfig()
	producerConfig.Endpoint = os.Getenv("LOG_TEST_ENDPOINT")
	provider := sls.NewStaticCredentialsProvider(os.Getenv("LOG_TEST_ACCESS_KEY_ID"), os.Getenv("LOG_TEST_ACCESS_KEY_SECRET"), "")
	producerConfig.CredentialsProvider = provider
	producerConfig.Region = os.Getenv("LOG_TEST_REGION")
	producerConfig.AuthVersion = sls.AuthV4
	producerInstance, err := NewProducer(producerConfig)
	if err != nil {
		panic(err)
	}

	producerInstance.Start() // 启动producer实例
	for i := 0; i < 100; i++ {
		// GenerateLog  is producer's function for generating SLS format logs
		log := GenerateLog(uint32(time.Now().Unix()), map[string]string{"content": "test", "content2": fmt.Sprintf("%v", i)})
		err := producerInstance.SendLog(os.Getenv("LOG_TEST_PROJECT"), os.Getenv("LOG_TEST_LOGSTORE"), "127.0.0.1", "topic", log)
		if err != nil {
			fmt.Println(err)
		}
	}
	producerInstance.Close(60)   // 有限关闭，传递int值，参数值需为正整数，单位为秒
	producerInstance.SafeClose() // 安全关闭
}

func printShardId(shardId string) string {
	config := GetDefaultProducerConfig()
	newShardHash, err := AdjustHash(shardId, config.Buckets)
	if err != nil {
		panic(err)
	}
	fmt.Printf("shardId: %s -> %s\n", shardId, newShardHash)
	return newShardHash
}

func TestProducer(t *testing.T) {
	config := GetDefaultProducerConfig()
	config.Endpoint = os.Getenv("LOG_TEST_ENDPOINT")
	provider := sls.NewStaticCredentialsProvider(os.Getenv("LOG_TEST_ACCESS_KEY_ID"), os.Getenv("LOG_TEST_ACCESS_KEY_SECRET"), "")
	config.CredentialsProvider = provider
	producerInstance, err := NewProducer(config)
	if err != nil {
		panic(err)
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	producerInstance.Start()
	hashs := []string{"aaa", ":bbbbb", "cccc"}
	for i := 0; i < 100; i++ {
		hash := hashs[i%len(hashs)]
		calc := printShardId(hash)
		content := []*sls.LogContent{}
		content = append(content, &sls.LogContent{
			Key:   proto.String("hash"),
			Value: proto.String(hash),
		})
		content = append(content, &sls.LogContent{
			Key:   proto.String("calc"),
			Value: proto.String(calc),
		})
		log := &sls.Log{
			Time:     proto.Uint32(uint32(time.Now().Unix())),
			Contents: content,
		}

		err := producerInstance.HashSendLog(os.Getenv("LOG_TEST_PROJECT"), os.Getenv("LOG_TEST_LOGSTORE"), hash, "127.0.0.1", "topic", log)
		if err != nil {
			fmt.Println(err)
		}
		time.Sleep(time.Millisecond * 500)
	}

	if _, ok := <-ch; ok {
		fmt.Println("Get the shutdown signal and start to shut down")
		producerInstance.Close(60000)
	}
}
