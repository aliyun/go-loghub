package sls

import (
	"crypto/md5"
	"fmt"
	"testing"
	"time"

	"github.com/Netflix/go-env"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SignerV1Suite struct {
	suite.Suite
	AccessKeyID     string
	AccessKeySecret string
	Endpoint        string
	env             TestEnvInfo
	signer          Signer
	client          ClientInterface
}

func (s *SignerV1Suite) SetupTest() {
	s.Endpoint = "cn-hangzhou.log.aliyuncs.com"
	s.AccessKeyID = "mockAccessKeyID"
	s.AccessKeySecret = "mockAccessKeySecret"
	s.signer = &SignerV1{
		accessKeyID:     s.AccessKeyID,
		accessKeySecret: s.AccessKeySecret,
	}
	_, err := env.UnmarshalFromEnviron(&s.env)
	s.Require().NoError(err)
	s.client = CreateNormalInterface(s.env.Endpoint,
		s.env.AccessKeyID,
		s.env.AccessKeySecret, "")
}

func (s *SignerV1Suite) TestSignatureGet() {
	headers := map[string]string{
		"x-log-apiversion":      "0.6.0",
		"x-log-signaturemethod": "hmac-sha256",
		"x-log-bodyrawsize":     "0",
		"Date":                  "Mon, 3 Jan 2010 08:33:47 GMT",
	}
	digest := "hNNf3Nv33R//Gxu++a0anEi7d5xbS5gapaPd/6eIxTk="
	expectedAuthStr := fmt.Sprintf("SLS %v:%v", s.AccessKeyID, digest)

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
		"x-log-signaturemethod": "hmac-sha256",
		"x-log-bodyrawsize":     "50",
		"Content-MD5":           md5Sum,
		"Content-Type":          "application/x-protobuf",
		"Content-Length":        "50",
		"Date":                  "Mon, 3 Jan 2010 08:33:47 GMT",
	}

	digest := "GGHiEECbn3P3QaMh2fLMs94z95xDVeQmhULhe54o0S4="
	err = s.signer.Sign("GET", "/logstores/app_log", h, body)
	if err != nil {
		assert.Fail(s.T(), err.Error())
	}
	expectedAuthStr := fmt.Sprintf("SLS %v:%v", s.AccessKeyID, digest)
	auth := h[HTTPHeaderAuthorization]
	assert.Equal(s.T(), expectedAuthStr, auth)
}

func (s *SignerV1Suite) TestSignV1Req() {
	p := s.env.ProjectName
	l := "test-signv1"
	exists, err := s.client.CheckProjectExist(p)
	s.Require().NoError(err)
	if !exists {
		_, err = s.client.CreateProject(p, "")
		s.Require().NoError(err)
	}
	exists, err = s.client.CheckLogstoreExist(p, l)
	s.Require().NoError(err)
	if !exists {
		err = s.client.CreateLogStore(p, l, 7, 1, false, 64)
		s.Require().NoError(err)
	}
	_, err = s.client.GetLogStore(p, l)
	s.Require().NoError(err)
	time.Sleep(time.Second * 5)
	t := uint32(time.Now().Unix())
	err = s.client.PutLogs(p, l, &LogGroup{
		Logs: []*Log{
			{
				Time: &t,
				Contents: []*LogContent{
					{
						Key:   proto.String("test"),
						Value: proto.String("test"),
					},
				},
			},
		},
	})
	s.Require().NoError(err)
	cursor, err := s.client.GetCursor(p, l, 0, "end")
	s.Require().NoError(err)
	cursorTime, err := s.client.GetCursorTime(p, l, 0, cursor)
	s.Require().NoError(err)
	s.Greater(cursorTime.Unix(), int64(0))
	s.client.DeleteLogStore(p, l)
}

func TestSignerV1Suite(t *testing.T) {
	suite.Run(t, new(SignerV1Suite))
}
