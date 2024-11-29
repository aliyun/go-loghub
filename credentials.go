package sls

import (
	"time"
)

type Credentials struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
}

// Expirable credentials with an expiration.
type tempCredentials struct {
	Credentials
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
	}
}

func (t *tempCredentials) isExpired() bool {
	return time.Now().After(t.Expiration)
}

func (t *tempCredentials) isValid() bool {
	return t.Credentials.AccessKeyID != "" && t.Credentials.AccessKeySecret != "" && !t.Expiration.IsZero()
}
