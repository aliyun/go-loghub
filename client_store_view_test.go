package sls

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type StoreViewTestSuite struct {
	suite.Suite
	endpoint        string
	project         string
	storeViewName   string
	accessKeyID     string
	accessKeySecret string
	client          Client
}

func TestStoreView(t *testing.T) {
	suite.Run(t, new(StoreViewTestSuite))
}

func (s *StoreViewTestSuite) SetupSuite() {
	s.endpoint = os.Getenv("LOG_TEST_ENDPOINT")
	s.project = os.Getenv("LOG_TEST_STORE_VIEW_PROJECT")
	s.accessKeyID = os.Getenv("LOG_TEST_ACCESS_KEY_ID")
	s.accessKeySecret = os.Getenv("LOG_TEST_ACCESS_KEY_SECRET")
	s.storeViewName = fmt.Sprintf("storeview_%d", time.Now().Unix())
	s.client.AccessKeyID = s.accessKeyID
	s.client.AccessKeySecret = s.accessKeySecret
	s.client.Endpoint = s.endpoint

	require.NotEmpty(s.T(), s.endpoint)
	require.NotEmpty(s.T(), s.project)
	require.NotEmpty(s.T(), s.accessKeyID)
	require.NotEmpty(s.T(), s.accessKeySecret)
	s.client.DeleteProject(s.project)

	time.Sleep(time.Second * 15)
	_, err := s.client.CreateProject(s.project, "test project")
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 1)
	s.createStores()
	time.Sleep(time.Second * 1)
}

func (s *StoreViewTestSuite) TearDownSuite() {
	err := s.client.DeleteProject(s.project)
	if err != nil {
		panic(err)
	}
}

func (s *StoreViewTestSuite) TestStoreViewCURD() {
	// get
	_, err := s.client.GetStoreView(s.project, s.storeViewName)
	s.NotNil(err)
	s.Require().True(strings.Contains(err.Error(), "not exist"))

	// list
	r, err := s.client.ListStoreViews(s.project, &ListStoreViewsRequest{
		Offset: 0,
		Size:   10,
	})
	s.Require().NoError(err)
	s.Require().Equal(0, r.Count)
	s.Require().Equal(0, r.Total)
	s.Require().Equal(0, len(r.StoreViews))

	// update
	err = s.client.UpdateStoreView(s.project, &StoreView{
		Name:      s.storeViewName,
		StoreType: STORE_VIEW_STORE_TYPE_LOGSTORE,
		Stores: []*StoreViewStore{
			{
				StoreName: "logstore-1",
				Project:   s.project,
			},
		},
	})
	s.NotNil(err)
	s.Require().True(strings.Contains(err.Error(), "not exist"))

	// delete
	err = s.client.DeleteStoreView(s.project, s.storeViewName)
	s.NotNil(err)
	s.Require().True(strings.Contains(err.Error(), "not exist"))

	// create ok
	err = s.client.CreateStoreView(s.project, &StoreView{
		Name:      s.storeViewName,
		StoreType: STORE_VIEW_STORE_TYPE_LOGSTORE,
		Stores: []*StoreViewStore{
			{
				StoreName: "logstore-1",
				Project:   s.project,
			},
		},
	})
	s.Require().NoError(err)

	// get
	storeView, err := s.client.GetStoreView(s.project, s.storeViewName)
	s.Require().NoError(err)
	s.Require().Equal(s.storeViewName, storeView.Name)
	s.Require().Equal(STORE_VIEW_STORE_TYPE_LOGSTORE, storeView.StoreType)
	s.Require().Equal(1, len(storeView.Stores))

	// list
	r, err = s.client.ListStoreViews(s.project, &ListStoreViewsRequest{
		Offset: 0,
		Size:   10,
	})
	s.Require().NoError(err)
	s.Require().Equal(1, r.Count)
	s.Require().Equal(1, r.Total)
	s.Require().Equal(1, len(r.StoreViews))
	s.Require().Equal(s.storeViewName, r.StoreViews[0])

	// update
	err = s.client.UpdateStoreView(s.project, &StoreView{
		Name:      s.storeViewName,
		StoreType: STORE_VIEW_STORE_TYPE_LOGSTORE,
		Stores: []*StoreViewStore{
			{
				StoreName: "logstore-1",
				Project:   s.project,
			},
		},
	})
	s.Require().NoError(err)

	// delete
	err = s.client.DeleteStoreView(s.project, s.storeViewName)
	s.Require().NoError(err)

	// get
	_, err = s.client.GetStoreView(s.project, s.storeViewName)
	s.NotNil(err)
	s.Require().True(strings.Contains(err.Error(), "not exist"))

	// list
	r, err = s.client.ListStoreViews(s.project, &ListStoreViewsRequest{
		Offset: 0,
		Size:   10,
	})
	s.Require().NoError(err)
	s.Require().Equal(0, r.Count)
	s.Require().Equal(0, r.Total)
	s.Require().Equal(0, len(r.StoreViews))
}

func (s *StoreViewTestSuite) TestStoreViewTypes() {
	err := s.client.CreateStoreView(s.project, &StoreView{
		Name:      s.storeViewName + "_1",
		StoreType: STORE_VIEW_STORE_TYPE_LOGSTORE,
		Stores: []*StoreViewStore{
			{
				Project:   s.project,
				StoreName: fmt.Sprintf("logstore-%d", 0),
			},
			{
				Project:   s.project,
				StoreName: fmt.Sprintf("logstore-%d", 1),
			},
		},
	})
	s.Require().NoError(err)

	err = s.client.CreateStoreView(s.project, &StoreView{
		Name:      s.storeViewName + "_2",
		StoreType: STORE_VIEW_STORE_TYPE_METRICSTORE,
		Stores: []*StoreViewStore{
			{
				Project:   s.project,
				StoreName: fmt.Sprintf("metricstore-%d", 0),
			},
			{
				Project:   s.project,
				StoreName: fmt.Sprintf("metricstore-%d", 1),
			},
		},
	})
	s.Require().NoError(err)

	storeview, err := s.client.GetStoreView(s.project, s.storeViewName+"_1")
	s.Require().NoError(err)
	s.Require().Equal(s.storeViewName+"_1", storeview.Name)
	s.Require().Equal(STORE_VIEW_STORE_TYPE_LOGSTORE, storeview.StoreType)
	s.Require().Equal(2, len(storeview.Stores))

	storeview, err = s.client.GetStoreView(s.project, s.storeViewName+"_2")
	s.Require().NoError(err)
	s.Require().Equal(s.storeViewName+"_2", storeview.Name)
	s.Require().Equal(STORE_VIEW_STORE_TYPE_METRICSTORE, storeview.StoreType)
	s.Require().Equal(2, len(storeview.Stores))

	// list
	r, err := s.client.ListStoreViews(s.project, &ListStoreViewsRequest{
		Offset: 0,
		Size:   10,
	})
	s.Require().NoError(err)
	s.Require().Equal(2, r.Count)
	s.Require().Equal(2, r.Total)
	s.Require().Equal(2, len(r.StoreViews))

	// get index
	r2, err := s.client.GetStoreViewIndex(s.project, s.storeViewName+"_1")
	s.Require().NoError(err)
	s.Require().Equal(2, len(r2.Indexes))
	s.Require().Equal(0, len(r2.StoreViewErrors))

	_, err = s.client.GetStoreViewIndex(s.project, s.storeViewName+"_2")
	s.Require().NotNil(err)
	s.True(strings.Contains(err.Error(), "not support"))

	// delete
	err = s.client.DeleteStoreView(s.project, s.storeViewName+"_1")
	s.Require().NoError(err)
	err = s.client.DeleteStoreView(s.project, s.storeViewName+"_2")
	s.Require().NoError(err)
}

func (s *StoreViewTestSuite) createStores() {
	for i := 0; i < 2; i++ {
		err := s.client.CreateLogStore(s.project, fmt.Sprintf("logstore-%d", i), 7, 2, false, 64)
		s.Require().NoError(err)
		err = s.client.CreateIndex(s.project, fmt.Sprintf("logstore-%d", i), Index{
			Line: &IndexLine{
				CaseSensitive: false,
				Chn:           false,
				Token: []string{
					",", " ", "'", "\"", ";", "\\", "$", "#", "!", "=", "(", ")", "[", "]", "{", "}", "?", "@", "&", "<", ">", "/", ":", "\n", "\t", "\r",
				},
			},
			LogReduce: false,
		})
		s.Require().NoError(err)
		err = s.client.CreateMetricStore(s.project, &LogStore{
			Name:       fmt.Sprintf("metricstore-%d", i),
			TTL:        7,
			ShardCount: 2,
		})
		s.Require().NoError(err)
	}

}
