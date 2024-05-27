module github.com/aliyun/aliyun-log-go-sdk

go 1.19

require (
	github.com/Netflix/go-env v0.0.0-20220526054621-78278af1949d
	github.com/alibabacloud-go/darabonba-openapi/v2 v2.0.4
	github.com/alibabacloud-go/sts-20150401/v2 v2.0.1
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58
	github.com/frankban/quicktest v1.10.2 // indirect
	github.com/go-kit/kit v0.10.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.4.2
	github.com/klauspost/compress v1.17.8
	github.com/pierrec/lz4 v2.6.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.5.1
	go.uber.org/atomic v1.5.0
	golang.org/x/net v0.0.0-20201021035429-f5854403a974
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

retract (
	v0.1.73
)
