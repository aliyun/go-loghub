package sls

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	env "github.com/Netflix/go-env"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	sts "github.com/alibabacloud-go/sts-20150401/v2/client"
	"github.com/stretchr/testify/assert"
)

func TestShouldRefresh(t *testing.T) {
	callCnt := 0
	now := time.Now()
	id, secret, token := "a1", "b1", "c1"
	expiration := now.Add(time.Hour)
	var mockErr error
	updateFunc := func() (string, string, string, time.Time, error) {
		callCnt++
		return id, secret, token, expiration, mockErr
	}
	adp := NewUpdateFuncProviderAdapter(updateFunc)
	assert.True(t, adp.shouldRefresh())
	cred := &tempCredentials{
		Credentials: Credentials{
			AccessKeyID:     id,
			AccessKeySecret: secret,
			SecurityToken:   token,
		},
		Expiration: expiration,
	}
	adp.cred.Store(cred)
	assert.False(t, adp.shouldRefresh())

	// expired
	cred.Expiration = now.Add(-time.Hour)
	adp.cred.Store(cred)
	assert.True(t, adp.shouldRefresh())

	// not expire but fetch ahead
	cred.Expiration = now.Add(-adp.fetchAhead).Add(-time.Second)
	adp.cred.Store(cred)
	assert.True(t, adp.shouldRefresh())
}

func TestUpdateFuncAdapter(t *testing.T) {
	callCnt := 0
	now := time.Now()
	id, secret, token := "a1", "b1", "c1"
	expiration := now.Add(time.Hour)
	var mockErr error
	updateFunc := func() (string, string, string, time.Time, error) {
		callCnt++
		return id, secret, token, expiration, mockErr
	}
	adp := NewUpdateFuncProviderAdapter(updateFunc)
	adpRetry := UPDATE_FUNC_RETRY_TIMES
	// first time fetch failed
	callCnt = 0
	mockErr = errors.New("mock err")
	{
		_, err := adp.GetCredentials()
		assert.Equal(t, 1+adpRetry, callCnt)
		assert.Error(t, err)
	}

	// first fetch success
	callCnt = 0
	mockErr = nil
	{
		cred, err := adp.GetCredentials()
		assert.Equal(t, 1, callCnt)
		assert.NoError(t, err)
		assert.Equal(t, cred.AccessKeyID, id)
		assert.Equal(t, cred.AccessKeySecret, secret)
		assert.Equal(t, cred.SecurityToken, token)
	}

	// fetch again, use cached cred
	callCnt = 0
	mockErr = nil
	id = "a2"
	{
		cred, err := adp.GetCredentials()
		assert.NoError(t, err)
		assert.Equal(t, 0, callCnt)
		assert.Equal(t, cred.AccessKeyID, "a1")
	}

	// expired, fetch new
	callCnt = 0
	mockErr = nil
	id = "a2"
	adp.cred.Load().(*tempCredentials).Expiration = now.Add(-time.Hour)
	{
		cred, err := adp.GetCredentials()
		assert.NoError(t, err)
		assert.Equal(t, 1, callCnt)
		assert.Equal(t, cred.AccessKeyID, "a2")
	}

	// fetch failed test, use last cred
	callCnt = 0
	adp.cred.Load().(*tempCredentials).Expiration = now.Add(-time.Hour)
	mockErr = errors.New("mock err")
	{
		cred, err := adp.GetCredentials()
		assert.NoError(t, err)
		assert.Equal(t, 1+adpRetry, callCnt)
		assert.Equal(t, cred.AccessKeyID, "a2")
	}

	callCnt = 0
	adp.cred.Load().(*tempCredentials).Expiration = expiration
	mockErr = nil
	{
		cred, err := adp.GetCredentials()
		assert.NoError(t, err)
		assert.Equal(t, 0, callCnt)
		assert.Equal(t, cred.AccessKeyID, "a2")
	}

	// fetch in advance, fetch a new one
	// use fetchCredentailsAhead
	callCnt = 0
	id = "a3"
	cred := adp.cred.Load().(*tempCredentials)
	adp.fetchAhead = time.Hour * 10
	cred.Expiration = now.Add(time.Hour)
	mockErr = nil
	{
		cred, err := adp.GetCredentials()
		assert.NoError(t, err)
		assert.Equal(t, 1, callCnt)
		assert.Equal(t, cred.AccessKeyID, "a3")
	}
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
