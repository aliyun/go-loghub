package sls

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/stretchr/testify/suite"
)

func TestAlert(t *testing.T) {
	suite.Run(t, new(AlertTestSuite))
	glog.Flush()
}

type AlertTestSuite struct {
	suite.Suite
	endpoint        string
	projectName     string
	logstoreName    string
	logShipperRole  string
	accessKeyID     string
	accessKeySecret string
	Project         *LogProject
	Logstore        *LogStore
	alertName       string
	dashboardName   string
	client          *Client
}

func (s *AlertTestSuite) SetupTest() {
	s.endpoint = os.Getenv("LOG_TEST_ENDPOINT")
	s.projectName = os.Getenv("LOG_TEST_PROJECT")
	s.logstoreName = os.Getenv("LOG_TEST_LOGSTORE")
	s.accessKeyID = os.Getenv("LOG_TEST_ACCESS_KEY_ID")
	s.accessKeySecret = os.Getenv("LOG_TEST_ACCESS_KEY_SECRET")
	slsProject, err := NewLogProject(s.projectName, s.endpoint, s.accessKeyID, s.accessKeySecret)
	s.Nil(err)
	s.NotNil(slsProject)
	s.Project = slsProject
	s.dashboardName = fmt.Sprintf("go-test-dashboard-%d", time.Now().Unix())
	s.alertName = fmt.Sprintf("go-test-alert-%d", time.Now().Unix())
	s.client = &Client{
		AccessKeyID:     s.accessKeyID,
		AccessKeySecret: s.accessKeySecret,
		Endpoint:        s.endpoint,
	}
}

func (s *AlertTestSuite) TestClient_CreateAlert() {
	err := s.createAlert()
	s.Require().Nil(err)
	err = s.client.DeleteAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
}

func (s *AlertTestSuite) createAlert() error {
	alerts, _, _, err := s.client.ListAlert(s.projectName, "", "", 0, 100)
	s.Require().Nil(err)
	for _, x := range alerts {
		err = s.client.DeleteAlert(s.projectName, x.Name)
		s.Require().Nil(err)
	}
	dashboard := Dashboard{
		DashboardName: s.dashboardName,
		DisplayName:   "test-dashboard",
		Description:   "test dashboard",
		ChartList:     []Chart{},
	}
	err = s.client.CreateDashboard(s.projectName, dashboard)
	if err != nil {
		slsErr := err.(*Error)
		if slsErr.Message != "specified dashboard already exists" {
			s.Require().Fail(slsErr.Message)
		}
	}
	alert := &Alert{
		Name:        s.alertName,
		State:       "Enabled",
		DisplayName: "AlertTest",
		Description: "Description for alert",
		Schedule: &Schedule{
			Type:     "FixedRate",
			Interval: "1h",
		},
		Configuration: &AlertConfiguration{
			QueryList: []*AlertQuery{
				{
					ChartTitle:   "chart-abc",
					Query:        "* | select count(1) as count",
					Start:        "-120s",
					End:          "now",
					TimeSpanType: "Custom",
					LogStore:     "test-logstore",
				},
			},
			Dashboard:  s.dashboardName,
			MuteUntil:  time.Now().Unix() + 3600,
			Throttling: "5m",
			Condition:  "count > 0",
			NotificationList: []*Notification{
				{
					Type:      NotificationTypeEmail,
					Content:   "${alertName} triggered at ${firetime}",
					EmailList: []string{"test@abc.com"},
				},
				{
					Type:       NotificationTypeSMS,
					Content:    "${alertName} triggered at ${firetime}",
					MobileList: []string{"1234567891"},
				},
			},
			NotifyThreshold: 1,
		},
	}
	return s.client.CreateAlert(s.projectName, alert)
}

func (s *AlertTestSuite) TestClient_UpdateAlert() {
	err := s.createAlert()
	s.Require().Nil(err)
	alert, err := s.client.GetAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	alert.DisplayName = "new display name"
	alert.CreateTime = 0
	alert.LastModifiedTime = 0
	err = s.client.UpdateAlert(s.projectName, alert)
	s.Require().Nil(err)
	alert, err = s.client.GetAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	s.Require().Equal("new display name", alert.DisplayName, "update alert failed")
	err = s.client.DeleteAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
}

func (s *AlertTestSuite) TestClient_DeleteAlert() {
	err := s.createAlert()
	s.Require().Nil(err)
	_, err = s.client.GetAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	err = s.client.DeleteAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	_, err = s.client.GetAlert(s.projectName, s.alertName)
	s.Require().NotNil(err)
}

func (s *AlertTestSuite) TestClient_DisableAndEnableAlert() {
	err := s.createAlert()
	s.Require().Nil(err)
	err = s.client.DisableAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	alert, err := s.client.GetAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	s.Require().Equal("Disabled", alert.State, "disable alert failed")
	err = s.client.EnableAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	alert, err = s.client.GetAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	s.Require().Equal("Enabled", alert.State, "enable alert failed")
	err = s.client.DeleteAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
}

func (s *AlertTestSuite) TestClient_GetAlert() {
	err := s.createAlert()
	s.Require().Nil(err)
	getAlert, err := s.client.GetAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
	s.Require().Equal(getAlert.Name, s.alertName)
	err = s.client.DeleteAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
}

func (s *AlertTestSuite) TestClient_ListAlert() {
	err := s.createAlert()
	s.Require().Nil(err)
	alerts, total, count, err := s.client.ListAlert(s.projectName, "", "", 0, 100)
	s.Require().Nil(err)
	if total != 1 || count != 1 {
		s.Require().Fail("list alert failed")
	}
	s.Require().Equal(1, len(alerts), "there should be only one alert")
	alert := alerts[0]
	s.Require().Equal(s.alertName, alert.Name, "list alert failed")
	err = s.client.DeleteAlert(s.projectName, s.alertName)
	s.Require().Nil(err)
}
