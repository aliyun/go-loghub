package sls

import (
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDNSCache(t *testing.T) {
	resolver := newDnsResolver()
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: newDnsDialContext(resolver, nil),
		},
		Timeout: defaultRequestTimeout,
	}
	_, err := httpClient.Get("https://www.baidu.com/")
	assert.Nil(t, err)
	assert.Equal(t, 1, resolver.GetCacheNum())
	_, err = httpClient.Get("https://www.baidu.com/")
	assert.Nil(t, err)
	assert.Equal(t, 1, resolver.GetCacheNum())
	_, err = httpClient.Get("https://www.aliyun.com/")
	assert.Nil(t, err)
	assert.Equal(t, 2, resolver.GetCacheNum())
	_, err = httpClient.Get("https://www.aliyun.com/")
	assert.Nil(t, err)
	assert.Equal(t, 2, resolver.GetCacheNum())
	_, err = httpClient.Get("https://cn.bing.com/")
	assert.Nil(t, err)
	assert.Equal(t, 3, resolver.GetCacheNum())
	_, err = httpClient.Get("https://cn.bing.com/")
	assert.Nil(t, err)
	assert.Equal(t, 3, resolver.GetCacheNum())
}

func TestTimeout(t *testing.T) {
	t.Skip("this will cost at least 3 minutes")
	resolver := newDnsResolver()
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: newDnsDialContext(resolver, &net.Dialer{
				Timeout: defaultRequestTimeout, // timeout 5s
			}),
		},
		Timeout: defaultRequestTimeout,
	}
	_, err := httpClient.Get("https://www.baidu.com/")
	assert.Nil(t, err)
	time.Sleep(1 * time.Minute)
	assert.Equal(t, 1, resolver.GetCacheNum())
	_, err = httpClient.Get("https://www.aliyun.com/")
	assert.Nil(t, err)
	time.Sleep(1 * time.Minute)
	assert.Equal(t, 2, resolver.GetCacheNum())
	resolver.deleteTimoutCachedIps(60)
	assert.Equal(t, 0, resolver.GetCacheNum())
	httpClient.Get("https://www.aliyun.com/")
	httpClient.Get("https://www.baidu.com/")
	assert.Equal(t, 2, resolver.GetCacheNum())
	time.Sleep(1 * time.Minute)
	_, err = httpClient.Get("https://cn.bing.com/")
	assert.Nil(t, err)
	assert.Equal(t, 3, resolver.GetCacheNum())
	resolver.deleteTimoutCachedIps(60)
	assert.Equal(t, 1, resolver.GetCacheNum())
}

func TestDNSCacheMultiThread(t *testing.T) {
	resolver := newDnsResolver()
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: newDnsDialContext(resolver, nil),
		},
		Timeout: defaultRequestTimeout,
	}
	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 5; j++ {
				_, err := httpClient.Get("https://www.baidu.com/")
				assert.Nil(t, err)
				_, err = httpClient.Get("https://www.aliyun.com/")
				assert.Nil(t, err)
				_, err = httpClient.Get("https://cn.bing.com/")
				assert.Nil(t, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
