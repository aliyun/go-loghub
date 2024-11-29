package sls

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientHttpClient(t *testing.T) {
	assert.NotEqual(t, defaultRequestTimeout, time.Second*33)
	{
		c := CreateNormalInterface("cn-hangzhou.log.aliyuncs.com", "", "", "")
		client := c.(*Client)
		assert.True(t, client.HTTPClient == defaultHttpClient || client.HTTPClient == nil)
		if client.HTTPClient != nil {
			transport := client.HTTPClient.Transport.(*http.Transport)
			assert.Equal(t, transport.IdleConnTimeout, defaultHTTPIdleTimeout)
			assert.Equal(t, defaultHttpClient.Timeout, defaultRequestTimeout)
		}
	}

	{
		// with proxy
		c := CreateNormalInterface("127.0.0.1", "", "", "")
		client := c.(*Client)
		assert.True(t, client.HTTPClient == defaultHttpClient || client.HTTPClient == nil)
		{
			transport := defaultHttpClient.Transport.(*http.Transport)
			assert.Equal(t, transport.IdleConnTimeout, defaultHTTPIdleTimeout)
			assert.Equal(t, defaultHttpClient.Timeout, defaultRequestTimeout)
		}

		p := convert(client, "test")
		assert.NotNil(t, p.httpClient)
		assert.NotEqual(t, p.httpClient, defaultHttpClient) // changed
		{
			transport := p.httpClient.Transport.(*http.Transport)
			assert.Equal(t, p.httpClient.Timeout, defaultRequestTimeout)
			assert.Equal(t, transport.IdleConnTimeout, defaultHTTPIdleTimeout)
			assert.NotNil(t, transport.Proxy)
		}
		p = p.WithRequestTimeout(time.Second * 33)
		assert.Equal(t, p.httpClient.Timeout, time.Second*33)
	}
}

func TestProjectHttpClient(t *testing.T) {
	assert.NotEqual(t, defaultRequestTimeout, time.Second*33)
	{
		p, err := NewLogProject("test-project", "cn-hangzhou.log.aliyuncs.com", "", "")
		assert.NoError(t, err)
		assert.Equal(t, p.httpClient, defaultHttpClient)
		transport := p.httpClient.Transport.(*http.Transport)
		assert.Equal(t, transport.IdleConnTimeout, defaultHTTPIdleTimeout)
		assert.Equal(t, defaultHttpClient.Timeout, defaultRequestTimeout)
		p = p.WithRequestTimeout(time.Second * 19)
		assert.Equal(t, p.httpClient.Timeout, time.Second*19)
	}

	{
		// with proxy
		p, err := NewLogProject("test-project", "127.0.0.1", "", "")
		assert.NoError(t, err)
		assert.NotEqual(t, p.httpClient, defaultHttpClient) // changed
		transport := p.httpClient.Transport.(*http.Transport)
		assert.Equal(t, p.httpClient.Timeout, defaultRequestTimeout)
		assert.Equal(t, transport.IdleConnTimeout, defaultHTTPIdleTimeout)
		assert.NotNil(t, transport.Proxy)
		p = p.WithRequestTimeout(time.Second * 19)
		assert.Equal(t, p.httpClient.Timeout, time.Second*19)
	}
}
