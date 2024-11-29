package sls

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/go-kit/kit/log/level"
)

type CredentialsProvider interface {
	/**
	 * GetCredentials is called everytime credentials are needed, the CredentialsProvider
	 * should cache credentials to avoid fetch credentials too frequently.
	 *
	 * @note GetCredentials must be thread-safe to avoid data race.
	 */
	GetCredentials() (Credentials, error)
}

// Create a static credential provider with AccessKeyID/AccessKeySecret/SecurityToken.
//
// Param accessKeyID and accessKeySecret must not be an empty string.
func NewStaticCredentialsProvider(accessKeyID, accessKeySecret, securityToken string) *StaticCredentialsProvider {
	return &StaticCredentialsProvider{
		Cred: Credentials{
			AccessKeyID:     accessKeyID,
			AccessKeySecret: accessKeySecret,
			SecurityToken:   securityToken,
		},
	}
}

/**
 * Create a new credentials provider, which uses a UpdateTokenFunction to fetch credentials.
 * @param updateFunc The function to fetch credentials. If the error returned is not nil, use last saved credentials.
 * This updateFunc will mayed called concurrently, and whill be called before the expiration of the last saved credentials.
 */
func NewUpdateFuncProviderAdapter(updateFunc UpdateTokenFunction) *UpdateFuncProviderAdapter {
	fetcher := fetcherWithRetry(updateFuncFetcher(updateFunc), UPDATE_FUNC_RETRY_TIMES)
	return &UpdateFuncProviderAdapter{
		fetcher:    fetcher,
		fetchAhead: defaultFetchAhead,
	}
}

/**
 * Create a credentials provider that uses ecs ram role, only works on ecs.
 *
 */
func NewEcsRamRoleCredentialsProvider(roleName string) CredentialsProvider {
	fetcherFunc := fetcherWithRetry(newEcsRamRoleFetcher(ECS_RAM_ROLE_URL_PREFIX, roleName, nil), ECS_RAM_ROLE_RETRY_TIMES)
	updateFunc := func() (accessKeyID, accessKeySecret, securityToken string, expireTime time.Time, err error) {
		cred, err := fetcherFunc()
		if err != nil {
			return "", "", "", time.Now(), err
		}
		return cred.AccessKeyID, cred.AccessKeySecret, cred.SecurityToken, cred.Expiration, nil
	}
	return NewUpdateFuncProviderAdapter(updateFunc)
}

/**
 * A static credetials provider that always returns the same long-lived credentials.
 * For back compatible.
 */
type StaticCredentialsProvider struct {
	Cred Credentials
}

func (p *StaticCredentialsProvider) GetCredentials() (Credentials, error) {
	return p.Cred, nil
}

type CredentialsFetcher = func() (*tempCredentials, error)

const UPDATE_FUNC_RETRY_TIMES = 3

// Adapter for porting UpdateTokenFunc to a CredentialsProvider.
type UpdateFuncProviderAdapter struct {
	cred atomic.Value // type *tempCredentials

	fetcher    CredentialsFetcher
	fetchAhead time.Duration
}

func updateFuncFetcher(updateFunc UpdateTokenFunction) CredentialsFetcher {
	return func() (*tempCredentials, error) {
		id, secret, token, expireTime, err := updateFunc()
		if err != nil {
			return nil, fmt.Errorf("updateTokenFunc fetch credentials failed: %w", err)
		}

		res := newTempCredentials(id, secret, token, expireTime, time.Now())
		if !res.isValid() {
			return nil, fmt.Errorf("updateTokenFunc result not valid, expirationTime:%s",
				expireTime.Format(time.RFC3339))
		}
		return res, nil
	}

}

// If credentials expires or will be exipred soon, fetch a new credentials and return it.
//
// Otherwise returns the credentials fetched last time.
//
// Retry at most maxRetryTimes if failed to fetch.
func (adp *UpdateFuncProviderAdapter) GetCredentials() (Credentials, error) {
	if !adp.shouldRefresh() {
		res := adp.cred.Load().(*tempCredentials)
		return res.Credentials, nil
	}
	level.Debug(Logger).Log("reason", "updateTokenFunc start to fetch new credentials")

	res, err := adp.fetcher() // res.lastUpdatedTime is not valid, do not use its

	if err == nil {
		copy := *res
		adp.cred.Store(&copy)

		if res.Expiration.Before(time.Now()) {
			level.Warn(Logger).Log("reason", "updateTokenFunc got a new credentials with expiration time before now",
				"nextExpiration", res.Expiration,
			)
		} else {
			level.Debug(Logger).Log("reason", "updateTokenFunc fetch new credentials succeed",
				"nextExpiration", res.Expiration,
			)
		}
		return res.Credentials, nil
	}

	lastCred := adp.cred.Load()
	// use last saved credentials when failed to fetch new credentials
	if lastCred != nil {
		return lastCred.(*tempCredentials).Credentials, nil
	}
	return Credentials{}, fmt.Errorf("updateTokenFunc fail to fetch credentials, err:%w", err)
}

var defaultFetchAhead = time.Minute * 2

func (adp *UpdateFuncProviderAdapter) shouldRefresh() bool {
	v := adp.cred.Load()
	if v == nil {
		return true
	}
	t := v.(*tempCredentials)
	if t.isExpired() {
		return true
	}
	return time.Now().Add(adp.fetchAhead).After(t.Expiration)
}

const ECS_RAM_ROLE_URL_PREFIX = "http://100.100.100.200/latest/meta-data/ram/security-credentials/"
const ECS_RAM_ROLE_RETRY_TIMES = 3

func newEcsRamRoleFetcher(urlPrefix, ramRole string, customClient *http.Client) CredentialsFetcher {
	return func() (*tempCredentials, error) {
		url := urlPrefix + ramRole
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("fail to build http request: %w", err)
		}

		var client *http.Client
		if customClient != nil {
			client = customClient
		} else {
			client = &http.Client{}
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fail to do http request: %w", err)
		}
		defer resp.Body.Close()
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("fail to read http resp body: %w", err)
		}
		fetchResp := ecsRamRoleHttpResp{}
		// 2. unmarshal
		err = json.Unmarshal(data, &fetchResp)
		if err != nil {
			return nil, fmt.Errorf("fail to unmarshal json: %w, body: %s", err, string(data))
		}
		// 3. check json param
		if !fetchResp.isValid() {
			return nil, fmt.Errorf("invalid fetch result, body: %s", string(data))
		}
		return newTempCredentials(
			fetchResp.AccessKeyID,
			fetchResp.AccessKeySecret,
			fetchResp.SecurityToken,
			fetchResp.Expiration,
			fetchResp.LastUpdated), nil
	}
}

// Response struct for http response of ecs ram role fetch request
type ecsRamRoleHttpResp struct {
	Code            string    `json:"Code"`
	AccessKeyID     string    `json:"AccessKeyId"`
	AccessKeySecret string    `json:"AccessKeySecret"`
	SecurityToken   string    `json:"SecurityToken"`
	Expiration      time.Time `json:"Expiration"`
	LastUpdated     time.Time `json:"LastUpdated"`
}

func (r *ecsRamRoleHttpResp) isValid() bool {
	return strings.ToLower(r.Code) == "success" && r.AccessKeyID != "" &&
		r.AccessKeySecret != "" && !r.Expiration.IsZero() && !r.LastUpdated.IsZero()
}

// Wraps a CredentialsFetcher with retry.
//
// @param retryTimes If <= 0, no retry will be performed.
func fetcherWithRetry(fetcher CredentialsFetcher, retryTimes int) CredentialsFetcher {
	return func() (*tempCredentials, error) {
		var errs []error
		for i := 0; i <= retryTimes; i++ {
			cred, err := fetcher()
			if err == nil {
				return cred, nil
			}
			errs = append(errs, err)
		}
		return nil, fmt.Errorf("exceed max retry times, last error: %w",
			joinErrors(errs...))
	}
}

// Replace this with errors.Join when go version >= 1.20
func joinErrors(errs ...error) error {
	if errs == nil {
		return nil
	}
	errStrs := make([]string, 0, len(errs))
	for _, e := range errs {
		errStrs = append(errStrs, e.Error())
	}
	return fmt.Errorf("[%s]", strings.Join(errStrs, ", "))
}
