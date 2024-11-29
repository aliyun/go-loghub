package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRegion(t *testing.T) {
	region, err := ParseRegion("xx-test-acdr-ut-1-intranet.log.aliyuncs.com")
	assert.NoError(t, err)
	assert.Equal(t, "xx-test-acdr-ut-1", region)

	region, err = ParseRegion("http://cn-hangzhou-intranet.log.aliyuncs.com")
	assert.NoError(t, err)
	assert.Equal(t, "cn-hangzhou", region)

	region, err = ParseRegion("https://cn-hangzhou.log.aliyuncs.com")
	assert.NoError(t, err)
	assert.Equal(t, "cn-hangzhou", region)

	region, err = ParseRegion("ap-southease-1-intranet.log.aliyuncs.com")
	assert.NoError(t, err)
	assert.Equal(t, "ap-southease-1", region)

	region, err = ParseRegion("cn-shanghai-corp.sls.aliyuncs.com")
	assert.NoError(t, err)
	assert.Equal(t, "cn-shanghai-corp", region)

	_, err = ParseRegion("sls.aliyuncs.com")
	assert.Error(t, err)
}
