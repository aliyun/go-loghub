package sls

import (
	"crypto/md5"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SignerV4Suite struct {
	suite.Suite
	mockAKID  string
	mockAKSec string
	uri       string
	method    string
	region    string
	body      string
	dateTime  string
	urlParams map[string]string
	headers   map[string]string
	signer    Signer
}

func toUriWithQuery(uri string, urlParams map[string]string) string {
	vals := url.Values{}
	for k, v := range urlParams {
		vals.Add(k, v)
	}
	return fmt.Sprintf("%s?%s", uri, vals.Encode())
}

func (s *SignerV4Suite) SetupTest() {
	s.mockAKID = "acsddda21dsd"
	s.mockAKSec = "zxasdasdasw2"
	s.uri = "/logstores"
	s.method = "POST"
	s.region = "cn-hangzhou"
	s.body = "adasd= -asd zcas"
	s.headers = map[string]string{
		"hello":      "world",
		"hello-Text": "a12X- ",
		" Ko ":       "",
		"":           "AA",
		"x-log-test": "het123",
		"x-acs-ppp":  "dds",
	}
	s.urlParams = map[string]string{
		" abc":  "efg",
		" agc ": "",
		"":      "efg",
		"A-bc":  "eFg",
	}
	s.dateTime = "20220808T032330Z"
	// Set dateTime for debugging
	s.headers[HTTPHeaderLogDate] = s.dateTime
	s.signer = &SignerV4{
		accessKeyID:     s.mockAKID,
		accessKeySecret: s.mockAKSec,
		region:          s.region,
	}
}

func (s *SignerV4Suite) TestSignV4Case1() {
	assert.Nil(s.T(), s.signer.Sign(s.method, toUriWithQuery(s.uri, s.urlParams), s.headers, []byte(s.body)))
	auth := s.headers[HTTPHeaderAuthorization]
	exp := "SLS4-HMAC-SHA256 " +
		"Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request," +
		"Signature=a98f5632e93836e63839cd836a54055f480020a9364ca944e2d34f2eb9bf1bed"
	assert.Equal(s.T(), exp, auth)
}

// Empty urlParams, empty headers, region cn-shanghai
func (s *SignerV4Suite) TestSignV4Case2() {
	s.region = "cn-shanghai"
	s.signer = &SignerV4{
		accessKeyID:     s.mockAKID,
		accessKeySecret: s.mockAKSec,
		region:          s.region,
	}
	s.headers = make(map[string]string)
	s.headers[HTTPHeaderLogDate] = s.dateTime

	assert.Nil(s.T(), s.signer.Sign(s.method, s.uri, s.headers, []byte(s.body)))
	auth := s.headers[HTTPHeaderAuthorization]
	exp := "SLS4-HMAC-SHA256 " +
		"Credential=acsddda21dsd/20220808/cn-shanghai/sls/aliyun_v4_request," +
		"Signature=8a10a5e723cb2e75964816de660b2c16a58af8bc0261f7f0722d832468c76ce8"
	assert.Equal(s.T(), exp, auth)
}

// Empty body
func (s *SignerV4Suite) TestSignV4Case3() {
	s.body = ""
	assert.Nil(s.T(), s.signer.Sign(s.method, toUriWithQuery(s.uri, s.urlParams), s.headers, []byte(s.body)))
	auth := s.headers[HTTPHeaderAuthorization]
	exp := "SLS4-HMAC-SHA256 " +
		"Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request," +
		"Signature=5a66d8f8051983e0e9d08e0f960ef9252ef971eead5bb5c7acec8617a2eb2701"
	assert.Equal(s.T(), exp, auth)
}

// Empty body and method get
func (s *SignerV4Suite) TestSignV4Case4() {
	s.body = ""
	s.method = "GET"
	assert.Nil(s.T(), s.signer.Sign(s.method, toUriWithQuery(s.uri, s.urlParams), s.headers, []byte(s.body)))
	auth := s.headers[HTTPHeaderAuthorization]
	exp := "SLS4-HMAC-SHA256 " +
		"Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request," +
		"Signature=d92741852500791d662a8d469ff61627c0559ecd86c3f59b7bf6772b6c62666a"
	assert.Equal(s.T(), exp, auth)
}

// Complex uri and urlParams
func (s *SignerV4Suite) TestSignV4Case5() {
	s.uri = "/logstores/hello/a+*~bb/cc"
	s.urlParams["abs-ij*asd/vc"] = "a~js+d ada"
	s.urlParams["a abAas123/vc"] = "a~jdad a2ADFs+d ada"
	assert.Nil(s.T(), s.signer.Sign(s.method, toUriWithQuery(s.uri, s.urlParams), s.headers, []byte(s.body)))
	auth := s.headers[HTTPHeaderAuthorization]
	exp := "SLS4-HMAC-SHA256 " +
		"Credential=acsddda21dsd/20220808/cn-hangzhou/sls/aliyun_v4_request," +
		"Signature=2c204068e961a8813a6bcf7ac422f7fa6e9bf9a5da493e0165dfe100854d18ff"
	assert.Equal(s.T(), exp, auth)
}

func (s *SignerV4Suite) TestSignV4Case6() {
	mockAKID := "mockAccessKeyID"
	mockAKSec := "mockAccessKeySecret"
	signer := SignerV4{accessKeyID: mockAKID, accessKeySecret: mockAKSec, region: "cn-hangzhou"}
	encoded := signer.percentEncode("123abc!@#$%^&*()-=_+ ~|\\/")
	assert.Equal(s.T(), "123abc%21%40%23%24%25%5E%26%2A%28%29-%3D_%2B%20~%7C%5C%2F", encoded)

	encoded = signer.percentEncode("!@#$%^&*()=-+ ~./_[()]%20你好\000\u0111❤😓")
	assert.Equal(s.T(), "%21%40%23%24%25%5E%26%2A%28%29%3D-%2B%20~.%2F_%5B%28%29%5D%2520%E4%BD%A0%E5%A5%BD%00%C4%91%E2%9D%A4%F0%9F%98%93", encoded)
}

func (s *SignerV4Suite) TestSignV1Case1() {
	headers := map[string]string{
		"x-log-apiversion":      "0.6.0",
		"x-log-signaturemethod": "hmac-sha256",
		"x-log-bodyrawsize":     "0",
		"Date":                  "Mon, 3 Jan 2010 08:33:47 GMT",
	}
	mockAKID := "mockAccessKeyID"
	mockAKSec := "mockAccessKeySecret"
	expSign := "hNNf3Nv33R//Gxu++a0anEi7d5xbS5gapaPd/6eIxTk="
	expAuth := fmt.Sprintf("SLS %s:%s", mockAKID, expSign)

	v1 := SignerV1{accessKeyID: mockAKID, accessKeySecret: mockAKSec}
	err := v1.Sign("GET", "/logstores", headers, nil)
	assert.Nil(s.T(), err)
	auth := headers[HTTPHeaderAuthorization]
	assert.Equal(s.T(), expAuth, auth)
}

// Protobuf content
func (s *SignerV4Suite) TestSignV1Case2() {
	body := []byte{10, 50, 10, 30, 8, 248, 178, 147,
		158, 5, 18, 22, 10, 7, 84, 101, 115, 116, 75,
		101, 121, 18, 11, 84, 101, 115, 116, 67, 111,
		110, 116, 101, 110, 116, 26, 0, 34, 14, 49,
		48, 46, 50, 51, 48, 46, 50, 48, 49, 46, 49, 49, 55}
	md5Sum := fmt.Sprintf("%X", md5.Sum(body))
	headers := map[string]string{
		"x-log-apiversion":      "0.6.0",
		"x-log-signaturemethod": "hmac-sha1",
		"x-log-bodyrawsize":     "50",
		"Content-MD5":           md5Sum,
		"Content-Type":          "application/x-protobuf",
		"Content-Length":        "50",
		"Date":                  "Mon, 3 Jan 2010 08:33:47 GMT",
	}
	mockAKID := "mockAccessKeyID"
	mockAKSec := "mockAccessKeySecret"
	expSign := "GGHiEECbn3P3QaMh2fLMs94z95xDVeQmhULhe54o0S4="
	expAuth := fmt.Sprintf("SLS %s:%s", mockAKID, expSign)
	v1 := SignerV1{
		accessKeyID:     mockAKID,
		accessKeySecret: mockAKSec,
	}
	err := v1.Sign("GET", "/logstores/app_log", headers, body)
	assert.Nil(s.T(), err)
	auth := headers[HTTPHeaderAuthorization]
	assert.Equal(s.T(), expAuth, auth)
}

func TestSignerV4Suite(t *testing.T) {
	suite.Run(t, new(SignerV4Suite))
}
