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
type TempCredentials struct {
	Credentials
	expiredFactor      float64
	expirationInMills  int64 // The time when the credentials expires, unix timestamp in millis
	lastUpdatedInMills int64
}

func NewTempCredentials(accessKeyId, accessKeySecret, securityToken string,
	expirationInMills, lastUpdatedInMills int64) *TempCredentials {

	return &TempCredentials{
		Credentials: Credentials{
			AccessKeyID:     accessKeyId,
			AccessKeySecret: accessKeySecret,
			SecurityToken:   securityToken,
		},
		expirationInMills:  expirationInMills,
		lastUpdatedInMills: lastUpdatedInMills,
		expiredFactor:      DEFAULT_EXPIRED_FACTOR,
	}
}

// @param factor must > 0.0 and <= 1.0
func (t *TempCredentials) WithExpiredFactor(factor float64) *TempCredentials {
	if factor > 0.0 && factor <= 1.0 {
		t.expiredFactor = factor
	}
	return t
}

// Returns true if credentials has expired already or will expire soon.
func (t *TempCredentials) ShouldRefresh() bool {
	now := time.Now().UnixMilli()
	if now >= t.expirationInMills {
		return true
	}
	duration := (float64)(t.expirationInMills-t.lastUpdatedInMills) * t.expiredFactor
	if duration < 0.0 { // check here
		duration = 0
	}
	return (now - t.lastUpdatedInMills) >= int64(duration)
}

// Returns true if credentials has expired already.
func (t *TempCredentials) HasExpired() bool {
	now := time.Now().UnixMilli()
	return now >= t.expirationInMills
}
