package util

import (
	sls "github.com/sanallen/aliyun-log-go-sdk"
)

// When you use the file under example, please configure the required variables here.
// Project define Project for test
var (
	ProjectName     = "ProjectName"
	Endpoint        = "Endpoint"
	LogStoreName    = "LogStoreName"
	AccessKeyID     = "AccessKeyID"
	AccessKeySecret = "AccessKeySecret"
	Client          sls.ClientInterface
)

// You can get the variable from the environment variable, or fill in the required configuration directly in the init function.
func init() {
	ProjectName = "your project name"
	AccessKeyID = "your ak id"
	AccessKeySecret = "your ak secret"
	Endpoint = "your endpoint" // just like cn-hangzhou.log.aliyuncs.com
	LogStoreName = "demo"

	Client = sls.CreateNormalInterface(Endpoint, AccessKeyID, AccessKeySecret, "")
}
