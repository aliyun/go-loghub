package producer

import (
	"reflect"
	"testing"
	"time"

	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
)

func TestGetTimeMs(t *testing.T) {
	type args struct {
		t int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{"TestGetTimeMs_1", args{1554880287052203000}, 1554880287052},
		{"TestGetTimeMs_2", args{1554880322922250000}, 1554880322922},
		{"TestGetTimeMs_3", args{1554880363658257000}, 1554880363658},
	}
	for _, tt := range tests {
		if got := GetTimeMs(tt.args.t); got != tt.want {
			t.Errorf("%q. GetTimeMs() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestGenerateLog(t *testing.T) {
	type args struct {
		logTime   uint32
		addLogMap map[string]string
	}
	content := []*sls.LogContent{
		&sls.LogContent{
			Key:   proto.String("name"),
			Value: proto.String("sls"),
		},
	}

	wantLog := &sls.Log{
		Time:     proto.Uint32(1554880724),
		Contents: content,
	}
	tests := []struct {
		name string
		args args
		want *sls.Log
	}{
		{"TestGenerateLog_1", args{1554880724, map[string]string{"name": "sls"}}, wantLog},
	}
	for _, tt := range tests {
		if got := GenerateLog(tt.args.logTime, tt.args.addLogMap); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. GenerateLog() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestGetLogSizeCalculate(t *testing.T) {
	log := GenerateLog(uint32(time.Now().Unix()), map[string]string{"content_1": "logtest", "contena_2": "logtest"})
	type args struct {
		log *sls.Log
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{"TestGetLogSizeCalculate_1", args{log}, 36},
	}
	for _, tt := range tests {
		if got := GetLogSizeCalculate(tt.args.log); got != tt.want {
			t.Errorf("%q. GetLogSizeCalculate() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
