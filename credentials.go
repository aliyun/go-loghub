package sls

import (
	"time"
)

type Credentials struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
}

const DEFAULT_EXPIRED_FACTOR = 0.8

// Expirable credentials with an expiration.
type tempCredentials struct {
	Credentials
	expiredFactor  float64
	Expiration     time.Time // The time when the credentials expires, unix timestamp in millis
	LastUpdateTime time.Time
}

func newTempCredentials(accessKeyId, accessKeySecret, securityToken string,
	expiration time.Time, lastUpdateTime time.Time) *tempCredentials {

	return &tempCredentials{
		Credentials: Credentials{
			AccessKeyID:     accessKeyId,
			AccessKeySecret: accessKeySecret,
			SecurityToken:   securityToken,
		},
		Expiration:     expiration,
		LastUpdateTime: lastUpdateTime,
		expiredFactor:  DEFAULT_EXPIRED_FACTOR,
	}
}

// @param factor must > 0.0 and <= 1.0, the less the factor is,
// the more frequently the credentials will be updated.
//
// If factor is set to 0, the credentials will be fetched every time
// [GetCredentials] is called.
//
// If factor is set to 1, the credentials will be fetched only when expired .
func (t *tempCredentials) WithExpiredFactor(factor float64) *tempCredentials {
	if factor > 0.0 && factor <= 1.0 {
		t.expiredFactor = factor
	}
	return t
}

// Returns true if credentials has expired already or will expire soon.
func (t *tempCredentials) ShouldRefresh() bool {
	now := time.Now()
	if now.Add(time.Minute * 2).After(t.Expiration) {
		return true
	}
	duration := (float64)(t.Expiration.Sub(t.LastUpdateTime)) * t.expiredFactor
	if duration < 0.0 { // check here
		duration = 0
	}
	return now.Sub(t.LastUpdateTime) > time.Duration(int64(duration))
}

// Returns true if credentials has expired already.
func (t *tempCredentials) HasExpired() bool {
	return time.Now().After(t.Expiration)
}
