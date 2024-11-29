package sls

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListLogStoreV2(t *testing.T) {
	client := CreateNormalInterfaceV2(os.Getenv("LOG_TEST_ENDPOINT"), NewStaticCredentialsProvider(
		os.Getenv("LOG_TEST_ACCESS_KEY_ID"),
		os.Getenv("LOG_TEST_ACCESS_KEY_SECRET"), ""))
	logstores, err := client.ListLogStoreV2(os.Getenv("LOG_TEST_PROJECT"), 0, 100, "")
	assert.NoError(t, err)
	assert.LessOrEqual(t, len(logstores), 100)
	fmt.Println(logstores)
}
