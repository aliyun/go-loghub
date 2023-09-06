package sls

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	env "github.com/Netflix/go-env"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	sts "github.com/alibabacloud-go/sts-20150401/v2/client"
	"github.com/stretchr/testify/assert"
)

func TestTempCred(t *testing.T) {
	now := time.Now()
	nowInMills := now.UnixMilli()

	// now = lastUpdated = expirationTime
	c := NewTempCredentials("", "", "", nowInMills, nowInMills)
	assert.True(t, c.ShouldRefresh())

	// now = lastUpdated < expirationTime
	oneHourInMills := int64(60 * 60 * 1000)
	c = NewTempCredentials("", "", "", nowInMills+oneHourInMills, nowInMills)
	assert.False(t, c.ShouldRefresh())

	// expirationTime < now  < lastUpdateTime
	c = NewTempCredentials("", "", "", nowInMills-oneHourInMills, nowInMills+oneHourInMills)
	assert.True(t, c.ShouldRefresh())
	// now < expirationTime < lastUpdateTime
	c = NewTempCredentials("", "", "", nowInMills+oneHourInMills, nowInMills+2*oneHourInMills)
	assert.False(t, c.ShouldRefresh())

	// lastUpdateTime < now < expirationTime and factored-expirationTime
	c = NewTempCredentials("", "", "", nowInMills+30*oneHourInMills, nowInMills-oneHourInMills)
	assert.False(t, c.ShouldRefresh())

	// lastUpdateTime < now < expirationTime , now > factored-expirationTime
	c = NewTempCredentials("", "", "", nowInMills+oneHourInMills, nowInMills-2*oneHourInMills).WithExpiredFactor(0.5)
	assert.True(t, c.ShouldRefresh())

}

func TestUpdateFuncAdapter(t *testing.T) {
	callCnt := 0
	now := time.Now()
	nowInMills := now.UnixMilli()
	hourInMills := int64(100000)
	id, secret, token := "a", "b", "c"
	expiration := now.Add(time.Hour)
	var err error
	updateFunc := func() (string, string, string, time.Time, error) {
		callCnt++
		return id, secret, token, expiration, err
	}
	adp := NewUpdateFuncProviderAdapter(updateFunc)
	adpRetry := UPDATE_FUNC_RETRY_TIMES
	cred, err2 := adp.GetCredentials()
	assert.Equal(t, 1, callCnt)
	assert.NoError(t, err2)
	assert.Equal(t, cred.AccessKeyID, id)
	assert.Equal(t, cred.AccessKeySecret, secret)
	assert.Equal(t, cred.SecurityToken, token)

	// not fetch new
	oldId := id
	id = "a2"
	cred, err2 = adp.GetCredentials()
	assert.NoError(t, err2)
	assert.Equal(t, 1, callCnt)
	assert.Equal(t, cred.AccessKeyID, oldId)

	// fetch new
	adp.expirationInMills.Store(nowInMills - hourInMills)
	cred, err2 = adp.GetCredentials()
	assert.NoError(t, err2)
	assert.Equal(t, 2, callCnt)
	assert.Equal(t, cred.AccessKeyID, id)

	// fetch failed test
	adp.expirationInMills.Store(nowInMills - hourInMills)
	err = errors.New("mock err")
	cred, err2 = adp.GetCredentials()
	assert.Error(t, err2)
	assert.Equal(t, 3+adpRetry, callCnt)
	assert.Equal(t, cred, Credentials{})

	// fetch in advance
	adp.advanceDuration = time.Hour * 10
	adp.expirationInMills.Store(nowInMills + hourInMills)
	err = nil
	cred, err2 = adp.GetCredentials()
	assert.NoError(t, err2)
	assert.Equal(t, 4+adpRetry, callCnt)
	assert.Equal(t, cred.AccessKeyID, id)
}

func TestBuilderParser(t *testing.T) {
	reqBuider := newEcsRamRoleReqBuilder(ECS_RAM_ROLE_URL_PREFIX, "test-ram-role")
	_, err := reqBuider()
	assert.NoError(t, err)

	body := ``
	resp := http.Response{
		Body: ioutil.NopCloser(bytes.NewBufferString(body)),
	}
	_, err = ecsRamRoleParser(&resp)
	assert.Error(t, err)
	body = `{"Code": "Success", "AccessKeyID": "xxxx", "AccessKeySecret": "yyyy",
		"SecurityToken": "zzzz", "Expiration": 234, "LastUpdated": 456
	}`
	resp = http.Response{
		Body: ioutil.NopCloser(bytes.NewBufferString(body)),
	}
	cred, err := ecsRamRoleParser(&resp)
	assert.NoError(t, err)
	assert.Equal(t, "xxxx", cred.AccessKeyID)
	assert.Equal(t, "yyyy", cred.AccessKeySecret)
	assert.Equal(t, int64(234), cred.expirationInMills)
}

type testCredentials struct {
	AccessKeyID     string `env:"LOG_TEST_ACCESS_KEY_ID"`
	AccessKeySecret string `env:"LOG_TEST_ACCESS_KEY_SECRET"`
	RoleArn         string `env:"LOG_TEST_ROLE_ARN"`
	Endpoint        string `env:"LOG_STS_TEST_ENDPOINT"`
}

func getStsClient(c *testCredentials) (*sts.Client, error) {
	conf := &openapi.Config{
		AccessKeyId:     &c.AccessKeyID,
		AccessKeySecret: &c.AccessKeySecret,
		Endpoint:        &c.Endpoint,
	}
	return sts.NewClient(conf)
}

// set env virables before test
func TestStsToken(t *testing.T) {
	c := testCredentials{}
	_, err := env.UnmarshalFromEnviron(&c)
	if err != nil {
		assert.Fail(t, "set ACCESS_KEY_ID/ACCESS_KEY_SECRET in environment first")
	}
	client, err := getStsClient(&c)
	assert.NoError(t, err)
	callCnt := 0
	updateFunc := func() (string, string, string, time.Time, error) {
		callCnt++
		name := "test-go-sdk-session"
		req := &sts.AssumeRoleRequest{
			RoleArn:         &c.RoleArn,
			RoleSessionName: &name,
		}
		resp, err := client.AssumeRole(req)
		assert.NoError(t, err)
		cred := resp.Body.Credentials
		e := cred.Expiration
		assert.NotNil(t, e)
		ex, err := time.Parse(time.RFC3339, *e)
		assert.NoError(t, err)
		return *cred.AccessKeyId, *cred.AccessKeySecret, *cred.SecurityToken, ex, nil
	}
	provider := NewUpdateFuncProviderAdapter(updateFunc)

	cred1, err := provider.GetCredentials()
	assert.NoError(t, err)
	assert.Equal(t, 1, callCnt)
	// fetch again, updateFunc not called, use cache
	cred2, err := provider.GetCredentials()
	assert.NoError(t, err)
	assert.EqualValues(t, cred1, cred2)
	assert.Equal(t, 1, callCnt)
	endpoint := os.Getenv("LOG_TEST_ENDPOINT")
	project := os.Getenv("LOG_TEST_PROJECT")
	client2 := CreateNormalInterfaceV2(endpoint, provider)
	res, err := client2.CheckProjectExist(project)
	assert.NoError(t, err)
	fmt.Println(res)
}

func TestTokenAutoUpdateClient(t *testing.T) {
	c := testCredentials{}
	_, err := env.UnmarshalFromEnviron(&c)
	if err != nil {
		assert.Fail(t, "set ACCESS_KEY_ID/ACCESS_KEY_SECRET in environment first")
	}
	client, err := getStsClient(&c)
	assert.NoError(t, err)
	endpoint := os.Getenv("LOG_TEST_ENDPOINT")
	project := os.Getenv("LOG_TEST_PROJECT")
	callCnt := 0
	updateFunc := func() (string, string, string, time.Time, error) {
		callCnt++
		name := "test-go-sdk-session"
		req := &sts.AssumeRoleRequest{
			RoleArn:         &c.RoleArn,
			RoleSessionName: &name,
		}
		resp, err := client.AssumeRole(req)
		assert.NoError(t, err)
		cred := resp.Body.Credentials
		e := cred.Expiration
		assert.NotNil(t, e)
		ex, err := time.Parse(time.RFC3339, *e)
		assert.NoError(t, err)
		return *cred.AccessKeyId, *cred.AccessKeySecret, *cred.SecurityToken, ex, nil
	}
	done := make(chan struct{})
	updateClient, err := CreateTokenAutoUpdateClient(endpoint, updateFunc, done)
	assert.NoError(t, err)
	res, err := updateClient.CheckProjectExist(project)
	assert.NoError(t, err)
	fmt.Println(res)
}
