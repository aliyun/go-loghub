This is a Golang SDK for Alibaba Cloud [Log Service](https://sls.console.aliyun.com/).

API Reference :

* [Chinese](https://help.aliyun.com/document_detail/29007.html)
* [English](https://intl.aliyun.com/help/doc-detail/29007.htm)

[![Build Status](https://travis-ci.org/aliyun/aliyun-log-go-sdk.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-log-go-sdk)
[![Coverage Status](https://coveralls.io/repos/github/aliyun/aliyun-log-go-sdk/badge.svg?branch=master)](https://coveralls.io/github/aliyun/aliyun-log-go-sdk?branch=master)


# Install Instruction

### LogHub Golang SDK

```
go get github.com/aliyun/aliyun-log-go-sdk
```

# Example 

### Write and Read LogHub

[loghub_sample.go](example/loghub/loghub_sample.go)

### Use Index on LogHub (SLS)

[index_sample.go](example/index/index_sample.go)

### Create Config for Logtail

[log_config_sample.go](example/config/log_config_plugin.go)

### Create Machine Group for Logtail

[machine_group_sample.go](example/machine_group/machine_group_sample.go)

# For developer
### Update log protobuf
`protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gofast_out=. log.proto`

### Use the go build tag to select json encoding/decoding

Allow users to actively select different json serialization packages at build time, the following four json serialization packages are supported

- github.com/aliyun/aliyun-log-go-sdk/internal/json: Standard package json serialization package, default use
- [json-iterator](https://github.com/json-iterator/go): Choose to use the `json-iterator` serialization package via `go build -tags jsoniter`
- [go-json](https://github.com/goccy/go-json): Choose to use the `go-json` serialization package via `go build -tags go_json`
- [sonic](https://github.com/bytedance/sonic): Choose to use the `sonic` serialization package via `go build -tags sonic`. This option uses cgo, so you need to set `CGO_ENABLE=1` at compile time, and requires a go version greater than 1.15
