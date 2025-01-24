package sls

import (
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SignerV1Suite struct {
	suite.Suite
	AccessKeyID     string
	AccessKeySecret string
	Endpoint        string

	signer Signer
}

func (s *SignerV1Suite) SetupTest() {
	s.Endpoint = "cn-hangzhou.log.aliyuncs.com"
	s.AccessKeyID = "mockAccessKeyID"
	s.AccessKeySecret = "mockAccessKeySecret"
	s.signer = &SignerV1{
		accessKeyID:     s.AccessKeyID,
		accessKeySecret: s.AccessKeySecret,
	}
}

func (s *SignerV1Suite) TestSignatureGet() {
	headers := map[string]string{
		"x-log-apiversion":      "0.6.0",
		"x-log-signaturemethod": "hmac-sha1",
		"x-log-bodyrawsize":     "0",
		"Date":                  "Mon, 3 Jan 2010 08:33:47 GMT",
	}
	digest := "Rwm6cTKzoti4HWoe+GKcb6Kv07E="
	expectedAuthStr := fmt.Sprintf("LOG %v:%v", s.AccessKeyID, digest)

	err := s.signer.Sign("GET", "/logstores", headers, nil)
	if err != nil {
		assert.Fail(s.T(), err.Error())
	}
	auth := headers[HTTPHeaderAuthorization]
	assert.Equal(s.T(), expectedAuthStr, auth)
}

func (s *SignerV1Suite) TestSignaturePost() {
	/*
	   topic=""
	   time=1405409656
	   source="10.230.201.117"
	   "TestKey": "TestContent"
	*/
	ct := &LogContent{
		Key:   proto.String("TestKey"),
		Value: proto.String("TestContent"),
	}
	lg := &Log{
		Time: proto.Uint32(1405409656),
		Contents: []*LogContent{
			ct,
		},
	}
	lgGrp := &LogGroup{
		Topic:  proto.String(""),
		Source: proto.String("10.230.201.117"),
		Logs: []*Log{
			lg,
		},
	}
	lgGrpLst := &LogGroupList{
		LogGroups: []*LogGroup{
			lgGrp,
		},
	}
	body, err := proto.Marshal(lgGrpLst)
	if err != nil {
		assert.Fail(s.T(), err.Error())
	}
	md5Sum := fmt.Sprintf("%X", md5.Sum(body))
	newLgGrpLst := &LogGroupList{}
	err = proto.Unmarshal(body, newLgGrpLst)
	if err != nil {
		assert.Fail(s.T(), err.Error())
	}
	h := map[string]string{
		"x-log-apiversion":      "0.6.0",
		"x-log-signaturemethod": "hmac-sha1",
		"x-log-bodyrawsize":     "50",
		"Content-MD5":           md5Sum,
		"Content-Type":          "application/x-protobuf",
		"Content-Length":        "50",
		"Date":                  "Mon, 3 Jan 2010 08:33:47 GMT",
	}

	digest := "87xQWqFaOSewqRIma8kPjGYlXHc="
	err = s.signer.Sign("GET", "/logstores/app_log", h, body)
	if err != nil {
		assert.Fail(s.T(), err.Error())
	}
	expectedAuthStr := fmt.Sprintf("LOG %v:%v", s.AccessKeyID, digest)
	auth := h[HTTPHeaderAuthorization]
	assert.Equal(s.T(), expectedAuthStr, auth)
}

func TestSignerV1Suite(t *testing.T) {
	suite.Run(t, new(SignerV1Suite))
}
