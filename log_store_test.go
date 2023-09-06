package sls

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Netflix/go-env"
	"github.com/stretchr/testify/suite"
)

func TestGetLogsTestSuite(t *testing.T) {
	suite.Run(t, new(GetLogsTestSuite))
}

type GetLogsTestSuite struct {
	suite.Suite
	env    TestEnvInfo
	client *Client
}

func (s *GetLogsTestSuite) SetupSuite() {
	_, err := env.UnmarshalFromEnviron(&s.env)
	s.Require().NoError(err)
	s.client = &Client{
		Endpoint:        s.env.Endpoint,
		AccessKeyID:     s.env.AccessKeyID,
		AccessKeySecret: s.env.AccessKeySecret,
		UserAgent:       DefaultLogUserAgent,
	}
}

func (s *GetLogsTestSuite) TearDownSuite() {

}

func (s *GetLogsTestSuite) TestGetLogsV2() {
	exists, err := s.client.CheckProjectExist(s.env.ProjectName)
	s.Require().NoError(err)
	s.True(exists)
	t := uint32(time.Now().Unix())
	timens := uint32(time.Now().UnixNano() % 1e9)
	key, val := "key1", "val1"
	lg := &LogGroup{
		Logs: []*Log{
			{
				Time:   &t,
				TimeNs: &timens,
				Contents: []*LogContent{
					{
						Key:   &key,
						Value: &val,
					},
					{
						Key:   &key,
						Value: &val,
					},
				},
			},
		},
	}
	// write log
	err = s.client.PostLogStoreLogs(s.env.ProjectName, s.env.LogstoreName, lg, nil)
	s.Require().NoError(err)
	// get logs
	time.Sleep(time.Second * 20)
	req := &GetLogRequest{
		From:  int64(t - 900),
		To:    int64(t + 10),
		Lines: 100,
		Query: "val1 | with_pack_meta",
	}
	// old
	ls := convertLogstore(s.client, s.env.ProjectName, s.env.LogstoreName)
	resp, err := getLogsV2Old(ls, req)
	s.Require().NoError(err)

	// new
	resp2, err := s.client.GetLogsV2(s.env.ProjectName, s.env.LogstoreName, req)
	s.Require().NoError(err)

	// these headers need not to be compared
	filtered := map[string]bool{}
	addFilter := func(key string) {
		filtered[strings.ToLower(key)] = true
	}
	addFilter(HTTPHeaderDate)
	addFilter(ElapsedMillisecond)
	addFilter(RequestIDHeader)
	addFilter(HTTPHeaderLogDate)
	addFilter("x-log-time")
	addFilter(HTTPHeaderContentType)
	addFilter(HTTPHeaderContentLength)
	addFilter(GetLogsQueryInfo)

	// compare headers
	for k := range resp.Header {
		key := strings.ToLower(k)
		if _, ok := filtered[key]; !ok {
			s.EqualValuesf(resp.Header.Get(k), resp2.Header.Get(k), "header key %s", k)
		}
	}
	// compare values
	s.EqualValues(resp.Progress, resp2.Progress)
	s.EqualValues(resp.Count, resp2.Count)
	s.EqualValues(resp.HasSQL, resp2.HasSQL)

	// compare query info
	queryInfoV2 := resp.Contents
	queryInfoV3 := resp2.Contents
	fmt.Println(queryInfoV2)
	fmt.Println(queryInfoV3)
	var info2 interface{}
	var info3 interface{}
	s.Require().NoError(json.Unmarshal([]byte(queryInfoV2), &info2))
	s.Require().NoError(json.Unmarshal([]byte(queryInfoV3), &info3))
	s.EqualValues(info2, info3)
}

func (s *GetLogsTestSuite) TestConstructQueryInfo() {
	v3Meta := &GetLogsV3ResponseMeta{
		Keys:            nil,
		Terms:           nil,
		Marker:          nil,
		Mode:            nil,
		PhraseQueryInfo: nil,
		Shard:           nil,
		ScanBytes:       nil,
		IsAccurate:      nil,
		ColumnTypes:     nil,
		Highlights:      nil,
	}
	contents, err := v3Meta.constructQueryInfo()
	s.Require().NoError(err)
	s.Equal("{}", contents)
	b := false
	v3Meta.IsAccurate = &b
	contents, err = v3Meta.constructQueryInfo()
	s.Require().NoError(err)
	s.Equal("{\"isAccurate\":0}", contents)
	b = true
	contents, err = v3Meta.constructQueryInfo()
	s.Require().NoError(err)
	s.Equal("{\"isAccurate\":1}", contents)

	v3Meta.Keys = make([]string, 0)
	shard := 0
	v3Meta.Shard = &shard
	contents, err = v3Meta.constructQueryInfo()
	s.Require().NoError(err)

	s.Equal("{\"shard\":0,\"isAccurate\":1}", contents)
}

func (s *GetLogsTestSuite) TestMarshalLines() {
	logs := []map[string]string{
		{
			"key1": "va1",
			"key2": " sdsadsa",
		},
		{
			"keOIIO y1": "NKJ*((*va1",
			"ke y2":     " sdsadsa",
		},
		{
			"keA DSy1": "va 2>>122e",
			"key2":     " sds2adsa",
		},
	}
	data, err := json.Marshal(logs)
	s.Require().NoError(err)
	var msg []json.RawMessage
	err = json.Unmarshal(data, &msg)
	s.Require().NoError(err)
}

func (s *GetLogsTestSuite) TestGetLogLinesV2() {
	exists, err := s.client.CheckProjectExist(s.env.ProjectName)
	s.Require().NoError(err)
	s.True(exists)
	t := uint32(time.Now().Unix())
	timens := uint32(time.Now().UnixNano() % 1e9)
	key, val := "key1", "val1"
	lg := &LogGroup{
		Logs: []*Log{
			{
				Time:   &t,
				TimeNs: &timens,
				Contents: []*LogContent{
					{
						Key:   &key,
						Value: &val,
					},
					{
						Key:   &key,
						Value: &val,
					},
				},
			},
		},
	}
	// write log
	err = s.client.PostLogStoreLogs(s.env.ProjectName, s.env.LogstoreName, lg, nil)
	s.Require().NoError(err)
	// get logs
	time.Sleep(time.Second * 20)
	req := &GetLogRequest{
		From:  int64(t - 900),
		To:    int64(t + 10),
		Lines: 100,
	}

	// new
	resp, err := s.client.GetLogLinesV2(s.env.ProjectName, s.env.LogstoreName, req)
	s.Require().NoError(err)
	s.Greater(resp.Count, int64(0))
	s.Equal("Complete", resp.Progress)
	s.Greater(len(resp.Lines), int(0))
	// fmt.Println(string(resp.Lines[0]))
}

// use HTTP GET, for testing
func getLogsV2Old(s *LogStore, req *GetLogRequest) (*GetLogsResponse, error) {
	rsp, b, logRsp, err := getLogsOld(s, req)
	if err == nil && len(b) != 0 {
		logs := []map[string]string{}
		err = json.Unmarshal(b, &logs)
		if err != nil {
			return nil, NewBadResponseError(string(b), rsp.Header, rsp.StatusCode)
		}
		logRsp.Logs = logs
	}
	return logRsp, err
}

// getLogs query logs with [from, to) time range
func getLogsOld(s *LogStore, req *GetLogRequest) (*http.Response, []byte, *GetLogsResponse, error) {

	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Accept":            "application/json",
	}

	urlVal := req.ToURLParams()

	uri := fmt.Sprintf("/logstores/%s?%s", s.Name, urlVal.Encode())
	r, err := request(s.project, "GET", uri, h, nil)
	if err != nil {
		return nil, nil, nil, NewClientError(err)
	}
	defer r.Body.Close()

	body, _ := ioutil.ReadAll(r.Body)
	if r.StatusCode != http.StatusOK {
		err := new(Error)
		if jErr := json.Unmarshal(body, err); jErr != nil {
			return nil, nil, nil, NewBadResponseError(string(body), r.Header, r.StatusCode)
		}
		return nil, nil, nil, err
	}

	count, err := strconv.ParseInt(r.Header.Get(GetLogsCountHeader), 10, 32)
	if err != nil {
		return nil, nil, nil, err
	}
	var contents string
	if _, ok := r.Header[GetLogsQueryInfo]; ok {
		if len(r.Header[GetLogsQueryInfo]) > 0 {
			contents = r.Header[GetLogsQueryInfo][0]
		}
	}
	hasSQL := false
	if r.Header.Get(HasSQLHeader) == "true" {
		hasSQL = true
	}

	return r, body, &GetLogsResponse{
		Progress: r.Header[ProgressHeader][0],
		Count:    count,
		Contents: contents,
		HasSQL:   hasSQL,
		Header:   r.Header,
	}, nil
}
