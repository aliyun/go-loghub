package sls

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	lz4 "github.com/pierrec/lz4"
	"github.com/stretchr/testify/suite"
)

func TestLogStore(t *testing.T) {
	suite.Run(t, new(LogstoreTestSuite))
}

type LogstoreTestSuite struct {
	suite.Suite
	endpoint        string
	projectName     string
	logstoreName    string
	accessKeyID     string
	accessKeySecret string
	Project         *LogProject
	Logstore        *LogStore
	client          ClientInterface
	arn             string
	cmkKeyId        string
	cmkEndpoint     string
}

func (s *LogstoreTestSuite) SetupSuite() {
	s.endpoint = os.Getenv("LOG_TEST_ENDPOINT")
	s.projectName = fmt.Sprintf("test-go-logstore-%d", time.Now().Unix())
	s.logstoreName = fmt.Sprintf("logstore-%d", time.Now().Unix())
	s.accessKeyID = os.Getenv("LOG_TEST_ACCESS_KEY_ID")
	s.accessKeySecret = os.Getenv("LOG_TEST_ACCESS_KEY_SECRET")
	s.arn = os.Getenv("LOG_TEST_ROLE_ARN")
	s.cmkKeyId = os.Getenv("LOG_TEST_CMK_ID")
	s.cmkEndpoint = os.Getenv("LOG_TEST_CMK_ENDPOINT")

	slsProject, err := NewLogProject(s.projectName, s.endpoint, s.accessKeyID, s.accessKeySecret)
	s.Nil(err)
	s.NotNil(slsProject)
	s.Project = slsProject
	s.Logstore = s.getLogstore()

	s.client = CreateNormalInterface(s.endpoint, s.accessKeyID, s.accessKeySecret, "")
	_, err = s.client.CreateProject(s.projectName, "")
	s.Nil(err)
	time.Sleep(10 * time.Second)
	logStore := s.getLogstore()
	err = s.Project.CreateLogStoreV2(logStore)
	s.Nil(err)
	time.Sleep(60 * time.Second)
}

func (s *LogstoreTestSuite) TearDownSuite() {
	err := s.client.DeleteProject(s.projectName)
	s.Require().Nil(err)
}

func (s *LogstoreTestSuite) getLogstore() *LogStore {
	return &LogStore{
		Name:          s.logstoreName,
		TTL:           7,
		ShardCount:    2,
		WebTracking:   false,
		AutoSplit:     true,
		MaxSplitShard: 16,
		AppendMeta:    false,
		project:       s.Project,
		EncryptConf: &EncryptConf{
			Enable:      true,
			EncryptType: "m4",
			UserCmkInfo: &EncryptUserCmkConf{
				CmkKeyId: s.cmkKeyId,
				Arn:      s.arn,
				RegionId: s.cmkEndpoint,
			},
		},
	}
}

func (s *LogstoreTestSuite) TestCreateLogStoreWithNewRequestBody() {

	store, err := s.Project.GetLogStore(s.logstoreName)
	s.Nil(err)
	s.Equal(s.logstoreName, store.Name)
	s.Equal(7, store.TTL)
	s.Equal(2, store.ShardCount)
	s.Equal(false, store.WebTracking)
	s.Equal(true, store.AutoSplit)
	s.Equal(16, store.MaxSplitShard)
	s.Equal(false, store.AppendMeta)
	s.Equal(0, store.HotTTL)
}

func (s *LogstoreTestSuite) TestCheckLogStore() {
	store, err := s.Project.GetLogStore(s.logstoreName)
	s.Nil(err)
	s.Equal(true, store.EncryptConf.Enable)
	s.Equal("m4", store.EncryptConf.EncryptType)
	s.Equal(s.cmkKeyId, store.EncryptConf.UserCmkInfo.CmkKeyId)
	s.Equal(s.arn, store.EncryptConf.UserCmkInfo.Arn)
	s.Equal(s.cmkEndpoint, store.EncryptConf.UserCmkInfo.RegionId)
}

func (s *LogstoreTestSuite) TestCheckLogstoreExist() {
	exist, err := s.Project.CheckLogstoreExist("not-exist-logstore")
	s.Nil(err)
	s.False(exist)
}

func (s *LogstoreTestSuite) TestCheckMachineGroupExist() {
	exist, err := s.Project.CheckMachineGroupExist("not-exist-group")
	s.Nil(err)
	s.False(exist)
}

func (s *LogstoreTestSuite) TestCheckConfigExist() {
	exist, err := s.Project.CheckConfigExist("not-exist-config")
	s.Nil(err)
	s.False(exist)
}

func (s *LogstoreTestSuite) TestPutLogs() {
	lg := generateLG()
	err := s.Logstore.PutLogs(lg)
	s.Nil(err)
}

func (s *LogstoreTestSuite) TestProjectNotExist() {
	projectName := "no-exist-project"
	slsProject, err := NewLogProject(projectName, s.endpoint, s.accessKeyID, s.accessKeySecret)
	s.Nil(err)
	slsLogstore, err := NewLogStore(s.logstoreName, slsProject)
	s.Nil(err)

	lg := generateLG()
	err = slsLogstore.PutLogs(lg)
	s.Require().NotNil(err)
	e, ok := err.(*Error)
	s.Require().True(ok)
	s.Require().Equal(e.Code, "ProjectNotExist")
	s.Require().Equal(e.HTTPCode, int32(404))
	s.Require().Equal(e.Message, fmt.Sprintf("The Project does not exist: %s", projectName))
}

func (s *LogstoreTestSuite) TestLogStoreNotExist() {
	logstoreName := "no-exist-logstore"
	slsLogstore, err := NewLogStore(logstoreName, s.Project)
	s.Nil(err)

	lg := generateLG()
	err = slsLogstore.PutLogs(lg)
	s.Require().NotNil(err)
	e, ok := err.(*Error)
	s.Require().True(ok)
	s.Require().Equal(e.Code, "LogStoreNotExist")
	s.Require().Equal(e.HTTPCode, int32(404))
}

func (s *LogstoreTestSuite) TestAccessIDNotExist() {
	accessID := "noExistKey"
	slsProject, err := NewLogProject(s.projectName, s.endpoint, accessID, s.accessKeySecret)
	s.Nil(err)
	slsLogstore, err := NewLogStore(s.logstoreName, slsProject)
	s.Nil(err)

	lg := generateLG()
	err = slsLogstore.PutLogs(lg)
	s.Require().NotNil(err)
	e, ok := err.(*Error)
	s.Require().True(ok)
	s.Require().Equal(e.Code, "Unauthorized")
	s.Require().Equal(e.HTTPCode, int32(401))
	s.Require().Equal(e.Message, fmt.Sprintf("AccessKeyId not found: %s", accessID))
}

func (s *LogstoreTestSuite) TestEmptyLogGroup() {
	lg := &LogGroup{
		Topic:  proto.String("test"),
		Source: proto.String("10.168.122.110"),
		Logs:   []*Log{},
	}
	err := s.Logstore.PutLogs(lg)
	s.Nil(err)
}

func (s *LogstoreTestSuite) TestPullLogs() {
	c := &LogContent{
		Key:   proto.String("error code"),
		Value: proto.String("InternalServerError"),
	}
	l := &Log{
		Time: proto.Uint32(uint32(time.Now().Unix())),
		Contents: []*LogContent{
			c,
		},
	}
	lg := &LogGroup{
		Topic:  proto.String("demo topic"),
		Source: proto.String("10.230.201.117"),
		Logs: []*Log{
			l,
		},
	}

	shards, err := s.Logstore.ListShards()
	s.True(len(shards) > 0)

	err = s.Logstore.PutLogs(lg)
	s.Nil(err)

	cursor, err := s.Logstore.GetCursor(0, "begin")
	s.Nil(err)
	endCursor, err := s.Logstore.GetCursor(0, "end")
	s.Nil(err)

	_, _, err = s.Logstore.PullLogs(0, cursor, "", 10)
	s.Nil(err)

	_, _, err = s.Logstore.PullLogs(0, cursor, endCursor, 10)
	s.Nil(err)
}

func (s *LogstoreTestSuite) TestGetLogByteWithError() {
	c := &LogContent{
		Key:   proto.String("error code"),
		Value: proto.String("InternalServerError"),
	}
	l := &Log{
		Time: proto.Uint32(uint32(time.Now().Unix())),
		Contents: []*LogContent{
			c,
		},
	}
	lg := &LogGroup{
		Topic:  proto.String("demo topic"),
		Source: proto.String("10.230.201.117"),
		Logs: []*Log{
			l,
		},
	}

	shards, err := s.Logstore.ListShards()
	s.True(len(shards) > 0)

	err = s.Logstore.PutLogs(lg)
	s.Nil(err)

	cursor, err := s.Logstore.GetCursor(0, "begin")
	s.Nil(err)
	endCursor, err := s.Logstore.GetCursor(0, "end")
	s.Nil(err)

	_, _, err = s.Logstore.GetLogsBytes(1000, cursor, "", 10)
	s.Contains(err.Error(), "ShardNotExist")
	s.NotNil(err)

	_, _, err = s.Logstore.GetLogsBytes(1000, cursor, endCursor, 10)
	s.Contains(err.Error(), "ShardNotExist")
	s.NotNil(err)
}

func (s *LogstoreTestSuite) TestGetLogs() {
	idx, err := s.Logstore.GetIndex()
	if err != nil {
		returnFlag := true
		if clientErr, ok := err.(*Error); ok {
			if clientErr.Code == "IndexConfigNotExist" {
				fmt.Printf("GetIndex success, no index config \n")
				returnFlag = false
			}
		}
		if returnFlag {
			fmt.Printf("GetIndex fail, err: %v, idx: %v\n", err, idx)
			return
		}
	} else {
		fmt.Printf("GetIndex success, idx: %v\n", idx)
	}
	idxConf := Index{
		Keys: map[string]IndexKey{},
		Line: &IndexLine{
			Token:         []string{",", ":", " "},
			CaseSensitive: false,
			IncludeKeys:   []string{},
			ExcludeKeys:   []string{},
		},
	}
	err = s.Logstore.CreateIndex(idxConf)
	fmt.Print(err)

	beginTime := uint32(time.Now().Unix())
	time.Sleep(60 * time.Second)
	c := &LogContent{
		Key:   proto.String("error code"),
		Value: proto.String("InternalServerError"),
	}
	l := &Log{
		Time: proto.Uint32(uint32(time.Now().Unix())),
		Contents: []*LogContent{
			c,
		},
	}
	lg := &LogGroup{
		Topic:  proto.String("demo topic"),
		Source: proto.String("10.230.201.117"),
		Logs:   []*Log{},
	}
	logCount := 50
	for i := 0; i < logCount; i++ {
		lg.Logs = append(lg.Logs, l)
	}

	putErr := s.Logstore.PutLogs(lg)
	s.Nil(putErr)

	time.Sleep(5 * time.Second)
	endTime := uint32(time.Now().Unix())

	time.Sleep(5 * time.Second)
	hResp, hErr := s.Logstore.GetHistograms("", int64(beginTime), int64(endTime), "InternalServerError")
	s.Nil(hErr)
	if hErr != nil {
		fmt.Printf("Get log error %v \n", hErr)
	}
	s.Equal(hResp.Count, int64(logCount))
	lResp, lErr := s.Logstore.GetLogs("", int64(beginTime), int64(endTime), "InternalServerError", 100, 0, false)
	s.Nil(lErr)
	s.Equal(lResp.Count, int64(logCount))
	fmt.Println(*lResp)
}

func (s *LogstoreTestSuite) TestLogstore() {
	logstoreName := "github-test"
	err := s.Project.DeleteLogStore(logstoreName)
	time.Sleep(5 * time.Second)
	err = s.Project.CreateLogStore(logstoreName, 14, 2, true, 16)
	s.Nil(err)
	time.Sleep(10 * time.Second)
	err = s.Project.UpdateLogStore(logstoreName, 7, 2)
	s.Nil(err)
	time.Sleep(1 * time.Second)
	logstores, err := s.Project.ListLogStore()
	s.Nil(err)
	s.True(len(logstores) >= 1)

	// test parameter "mode" of logstore, default mode is "standard"
	time.Sleep(time.Second)
	logstore, err := s.Project.GetLogStore(logstoreName)
	s.Nil(err)
	s.Equal(logstore.Mode, "standard")

	time.Sleep(time.Second)
	configs, configCount, err := s.Project.ListConfig(0, 100)
	s.Nil(err)
	s.True(len(configs) >= 0)
	s.Equal(len(configs), configCount)
	machineGroups, machineGroupCount, err := s.Project.ListMachineGroup(0, 100)
	s.Nil(err)
	s.True(len(machineGroups) >= 0)
	s.Equal(len(machineGroups), machineGroupCount)
	err = s.Project.DeleteLogStore(logstoreName)
	s.Nil(err)
}

func (s *LogstoreTestSuite) TestLogstoreLiteMode() {
	logstoreName := "github-test"
	_ = s.Project.DeleteLogStore(logstoreName)
	// create a "lite" mode logstore
	lite := &LogStore{
		Name:          logstoreName,
		TTL:           14,
		ShardCount:    2,
		AutoSplit:     true,
		MaxSplitShard: 16,
		Mode:          "lite",
	}
	err := s.Project.CreateLogStoreV2(lite)
	s.Nil(err)
	time.Sleep(10 * time.Second)

	// check if logstore is in "lite" mode
	liteResp, err := s.Project.GetLogStore(logstoreName)
	s.Nil(err)
	s.Equal(liteResp.Mode, "lite")

	// clean
	err = s.Project.DeleteLogStore(logstoreName)
	s.Nil(err)
}

func generateLG() *LogGroup {
	content := &LogContent{
		Key:   proto.String("demo_key"),
		Value: proto.String("demo_value"),
	}
	logRecord := &Log{
		Time:     proto.Uint32(uint32(time.Now().Unix())),
		Contents: []*LogContent{content},
	}
	lg := &LogGroup{
		Topic:  proto.String("test"),
		Source: proto.String("10.168.122.110"),
		Logs:   []*Log{logRecord},
	}
	return lg
}

func (s *LogstoreTestSuite) TestLogStoreReadErrorMock() {
	topic := ""
	begin_time := uint32(time.Now().Unix())
	from := int64(begin_time)
	to := int64(begin_time + 2)
	queryExp := "InternalServerError"
	maxLineNum := 100
	offset := 0
	reverse := false

	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Accept":            "application/json",
	}

	uri := fmt.Sprintf("/logstores/%v?type=log&topic=%v&from=%v&to=%v&query=%v&line=%v&offset=%v&reverse=%v", s.Logstore.Name, topic, from, to, queryExp, maxLineNum, offset, reverse)

	mockErr := new(mockErrorRetry)
	mockErr.RetryCnt = 10000000
	serverError := Error{}
	serverError.Message = "server error 500"
	serverError.HTTPCode = int32(500)
	mockErr.Err = serverError

	//发生retry，一直retry不成功，err结果为retry超时
	r1, err := request(s.Logstore.project, "GET", uri, h, nil, mockErr)
	s.Nil(r1)
	s.NotNil(err)
	s.True(strings.Contains(string(err.Error()), "context deadline exceeded"))
	s.True(strings.Contains(string(err.Error()), "server error 500"))
	s.True(strings.Contains(string(err.Error()), "stopped retrying err"))

	//err为不符合retry条件的情况, 返回预期的err
	mockErr.Err.HTTPCode = int32(404)
	mockErr.Err.Message = "server error 404"
	r2, err2 := request(s.Logstore.project, "GET", uri, h, nil, mockErr)
	s.Nil(r2)
	s.NotNil(err2)
	s.False(strings.Contains(string(err2.Error()), "stopped retrying err"))
	s.False(strings.Contains(string(err2.Error()), "context deadline exceeded"))
	s.True(strings.Contains(string(err2.Error()), "server error 404"))

	//err为nil的情况，没有retry发生, 模拟重试一次
	mockErr.Err.HTTPCode = int32(200)
	mockErr.RetryCnt = 1
	r3, err3 := request(s.Logstore.project, "GET", uri, h, nil, mockErr)
	s.NotNil(r3)
	s.Nil(err3)

	// 发生retry，retry几次之后成功了
	mockErr.Err.Message = "server error 500"
	mockErr.Err.HTTPCode = int32(500)
	mockErr.RetryCnt = 3

	r4, err4 := request(s.Logstore.project, "GET", uri, h, nil, mockErr)
	s.NotNil(r4)
	s.Nil(err4)

}

func (s *LogstoreTestSuite) TestLogStoreWriteErrorMock() {
	c := &LogContent{
		Key:   proto.String("error code"),
		Value: proto.String("InternalServerError"),
	}
	l := &Log{
		Time: proto.Uint32(uint32(time.Now().Unix())),
		Contents: []*LogContent{
			c,
		},
	}
	lg := &LogGroup{
		Topic:  proto.String("demo topic"),
		Source: proto.String("10.230.201.117"),
		Logs: []*Log{
			l,
		},
	}

	body, _ := proto.Marshal(lg)

	// Compresse body with lz4
	var hashTable [1 << 16]int
	out := make([]byte, lz4.CompressBlockBound(len(body)))
	n, _ := lz4.CompressBlock(body, out, hashTable[:])

	h := map[string]string{
		"x-log-compresstype": "lz4",
		"x-log-bodyrawsize":  fmt.Sprintf("%v", len(body)),
		"Content-Type":       "application/x-protobuf",
	}

	uri := fmt.Sprintf("/logstores/%v", s.Logstore.Name)

	mockErr := new(mockErrorRetry)
	mockErr.RetryCnt = 10000000
	serverError := Error{}
	serverError.Message = "server error 502"
	serverError.HTTPCode = int32(502)
	mockErr.Err = serverError

	//发生retry，一直retry不成功，err结果为retry超时
	r, err := request(s.Logstore.project, "POST", uri, h, out[:n], mockErr)
	s.Nil(r)
	s.NotNil(err)
	s.True(strings.Contains(string(err.Error()), "context deadline exceeded"))
	s.True(strings.Contains(string(err.Error()), "server error 502"))
	s.True(strings.Contains(string(err.Error()), "stopped retrying err"))

	//err为不符合retry条件的情况, 返回预期的err
	mockErr.Err.HTTPCode = int32(504)
	mockErr.Err.Message = "server error 504"
	r2, err2 := request(s.Logstore.project, "POST", uri, h, out[:n], mockErr)

	s.Nil(r2)
	s.NotNil(err2)
	s.True(strings.Contains(string(err2.Error()), "server error 504"))
	s.False(strings.Contains(string(err2.Error()), "stopped retrying err"))
	s.False(strings.Contains(string(err2.Error()), "context deadline exceeded"))

	//err为nil的情况，没有retry发生
	mockErr.Err.HTTPCode = int32(200)
	mockErr.RetryCnt = 1
	r3, err3 := request(s.Logstore.project, "POST", uri, h, out[:n], mockErr)

	s.NotNil(r3)
	s.Nil(err3)
	r = &http.Response{}

	mockErr.Err.Message = "server error 503"
	mockErr.Err.HTTPCode = int32(503)
	mockErr.RetryCnt = 3

	r4, err4 := request(s.Logstore.project, "POST", uri, h, out[:n], mockErr)
	s.NotNil(r4)
	s.Nil(err4)
}

func (s *LogstoreTestSuite) TestReqTimeoutRetry() {
	assert := s.Require()

	requestTimeout := 1 * time.Second
	retryTimeout := 3 * time.Second

	count := 0
	ts := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				count++
				time.Sleep(3 * time.Second)
			}),
	)
	defer ts.Close()

	slsProject, err := NewLogProject("my-project", ts.URL, "id", "key")
	slsProject.WithRequestTimeout(requestTimeout).WithRetryTimeout(retryTimeout)
	assert.Nil(err)
	assert.NotNil(slsProject)

	slsLogstore, err := NewLogStore("my-store", slsProject)
	assert.Nil(err)
	assert.NotNil(slsLogstore)

	_, err = slsLogstore.ListShards()
	assert.NotNil(err)
	assert.Contains(err.Error(), "context deadline exceeded")
	assert.True(count >= 2, fmt.Sprintf("count: %d", count))
}

func (s *LogstoreTestSuite) TestLogShipper() {
	assert := s.Require()

	ossShipperName := "github-test-shipper"
	// In case shipper exists
	s.Logstore.DeleteShipper(ossShipperName)

	storage := ShipperStorage{
		Format: "json",
		Detail: map[string]interface{}{"enableTag": true},
	}
	ossShipperConfig := &OSSShipperConfig{
		OssBucket:      "test_bucket",
		OssPrefix:      "testPrefix",
		RoleArn:        s.arn,
		BufferInterval: 300,
		BufferSize:     100,
		CompressType:   "none",
		PathFormat:     "%Y/%m/%d/%H/%M",
		Storage:        storage,
	}
	ossShipper := &Shipper{
		ShipperName:         ossShipperName,
		TargetType:          OSSShipperType,
		TargetConfiguration: ossShipperConfig,
	}
	err := s.Logstore.CreateShipper(ossShipper)
	assert.Nil(err)
	getShipper, err := s.Logstore.GetShipper(ossShipperName)
	assert.Nil(err)
	assert.Equal(ossShipperConfig, getShipper.TargetConfiguration)
	assert.Equal(ossShipperName, getShipper.ShipperName)
	assert.Equal(OSSShipperType, getShipper.TargetType)
	shipperList, err := s.Logstore.ListShipper()
	assert.Equal(1, len(shipperList))

	ossShipperConfig.OssPrefix = "newPrefix"
	err = s.Logstore.UpdateShipper(ossShipper)
	assert.Nil(err)
	getShipper, err = s.Logstore.GetShipper(ossShipperName)
	assert.Nil(err)
	assert.Equal(ossShipperConfig, getShipper.TargetConfiguration)
	assert.Equal(ossShipperName, getShipper.ShipperName)
	assert.Equal(OSSShipperType, getShipper.TargetType)

	detail := OssStoreageCsvDetail{
		Delimiter:      ",",
		Header:         false,
		LineFeed:       "\n",
		Columns:        []string{"__topic__", "alarm_count", "alarm_message", "alarm_type", "category", "project_name"},
		NullIdentifier: "",
		Quote:          "\\",
	}
	storage = ShipperStorage{
		Format: "csv",
		Detail: detail,
	}
	ossShipperConfig.Storage = storage
	ossShipper.TargetConfiguration = ossShipperConfig
	err = s.Logstore.UpdateShipper(ossShipper)
	assert.Nil(err)

	config1 := ParquetConfig{
		Name: "name",
		Type: "string",
	}
	config2 := ParquetConfig{
		Name: "name2",
		Type: "int64",
	}
	ParquetDetail := OssStoreageParquet{}
	ParquetDetail.Columns = append(ParquetDetail.Columns, config1)
	ParquetDetail.Columns = append(ParquetDetail.Columns, config2)
	storage = ShipperStorage{
		Format: "parquet",
		Detail: ParquetDetail,
	}
	ossShipperConfig.Storage = storage
	ossShipper.TargetConfiguration = ossShipperConfig
	err = s.Logstore.UpdateShipper(ossShipper)
	assert.Nil(err)
	err = s.Logstore.DeleteShipper(ossShipperName)
	assert.Nil(err)
	_, err = s.Logstore.GetShipper(ossShipperName)
	assert.NotNil(err)
	assert.IsType(new(Error), err)
	assert.Equal(int32(404), err.(*Error).HTTPCode)
}
