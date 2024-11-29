package main

import (
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

func main() {
	accessKeyID := ""
	accessKeySecret := ""
	project := "k8s-log-cdc990939f2f547e883a4cb9236e85872"
	logstore := "002"
	dashboardName := "dashboardtest"
	alertName := "test-alert"
	client := sls.CreateNormalInterface(
		"cn-hangzhou.log.aliyuncs.com",
		accessKeyID,
		accessKeySecret,
		"",
	)
	chart := sls.Chart{
		Title: "chart-1234567",
		Type:  "table",
		Search: sls.ChartSearch{
			Logstore: logstore,
			Topic:    "",
			Query:    "* | select count(1) as count",
			Start:    "-300s",
			End:      "now",
		},
		Display: sls.ChartDisplay{
			XPosition:   0,
			YPosition:   -1,
			Width:       5,
			Height:      5,
			DisplayName: "chart-test",
		},
	}
	dashboard := sls.Dashboard{
		DashboardName: dashboardName,
		DisplayName:   "test-dashboard",
		Description:   "test dashboard",
		ChartList: []sls.Chart{
			chart,
		},
	}
	err := client.CreateDashboard(project, dashboard)
	if err != nil {
		panic(err)
	}
	alert := &sls.Alert{
		Name:        alertName,
		DisplayName: "count monitoring",
		Description: "",
		State:       "Enabled",
		Status:      "",
		Configuration: &sls.AlertConfiguration{
			Condition: "count > 0",
			Dashboard: dashboardName,
			QueryList: []*sls.AlertQuery{
				{
					ChartTitle:   chart.Title,
					LogStore:     logstore,
					Query:        chart.Search.Query,
					TimeSpanType: "Custom",
					Start:        chart.Search.Start,
					End:          chart.Search.End,
				},
			},
			MuteUntil: time.Now().Unix() + 10,
			NotificationList: []*sls.Notification{
				{
					Type:      sls.NotificationTypeEmail,
					Content:   "${alertName} triggered at ${firetime}",
					EmailList: []string{"abc@test.com"},
				},
				{
					Type:       sls.NotificationTypeDingTalk,
					Content:    "${alertName} triggered at ${firetime}",
					ServiceUri: "https://oapi.dingtalk.com/robot/send?access_token=xxx",
				},
				{
					Type:       sls.NotificationTypeSMS,
					Content:    "${alertName} triggered at ${firetime}",
					MobileList: []string{"12345670000"},
				},
				{
					Type:       sls.NotificationTypeWebhook,
					Method:     "OPTIONS",
					Content:    "${alertName} triggered at ${firetime}",
					Headers:    map[string]string{"content-type": "test", "name": "aliyun"},
					ServiceUri: "https://www.aliyun.com/",
				},
			},
			NotifyThreshold: 1,
			Throttling:      "5m",
		},
		Schedule: &sls.Schedule{
			Type:     sls.ScheduleTypeFixedRate,
			Interval: "60s",
		},
	}
	err = client.CreateAlert(project, alert)
	if err != nil {
		panic(err)
	}
}
