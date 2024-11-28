package sls

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestIndex_MarshalJSON(t *testing.T) {

	type fields struct {
		Keys                   map[string]IndexKey
		Line                   *IndexLine
		Ttl                    uint32
		MaxTextLen             uint32
		LogReduce              bool
		LogReduceWhiteListDict []string
		LogReduceBlackListDict []string
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			name: "keys and line",
			fields: fields{
				Keys: map[string]IndexKey{
					"test1": {},
				},
				Line: &IndexLine{},
			},
			want: []byte(`{"keys":{"test1":{"token":null,"caseSensitive":false,"type":"","chn":false}},"line":{"token":null,"caseSensitive":false,"chn":false},"log_reduce":false}`),
		},
		{
			name: "only ttl",
			fields: fields{
				Ttl:        2,
				MaxTextLen: 3,
			},
			want: []byte(`{"ttl":2,"max_text_len":3,"log_reduce":false}`),
		},
		{
			name: "white & black",
			fields: fields{
				LogReduceWhiteListDict: []string{"key1"},
				LogReduceBlackListDict: []string{"key2"},
				LogReduce:              true,
			},
			want: []byte(`{"log_reduce":true,"log_reduce_white_list":["key1"],"log_reduce_black_list":["key2"]}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &Index{
				Keys:                   tt.fields.Keys,
				Line:                   tt.fields.Line,
				Ttl:                    tt.fields.Ttl,
				MaxTextLen:             tt.fields.MaxTextLen,
				LogReduce:              tt.fields.LogReduce,
				LogReduceWhiteListDict: tt.fields.LogReduceWhiteListDict,
				LogReduceBlackListDict: tt.fields.LogReduceBlackListDict,
			}
			got, _ := json.Marshal(u)
			fmt.Printf("%s", got)
			assert.Equalf(t, tt.want, got, "MarshalJSON()")
		})
	}

}

func TestLogGroupIdentity(t *testing.T) {

	sampleLog := &Log{
		Time: proto.Uint32(1732774880),
		Contents: []*LogContent{
			{
				Key:   proto.String("test"),
				Value: proto.String("test"),
			},
		},
	}
	logGroupList := &LogGroupList{
		LogGroups: []*LogGroup{
			{
				Logs: []*Log{sampleLog},
			},
			{
				Logs: []*Log{sampleLog, sampleLog},
			},
			{
				Logs: []*Log{sampleLog, sampleLog, sampleLog},
			},
		},
	}
	err := logGroupList.addIdIfPossible("MTcyOTA3MDYxODQyODA0NDY1NQ==", 0)
	assert.NoError(t, err)

	assert.Equal(t, "0|MTcyOTA3MDYxODQyODA0NDY1NQ==", logGroupList.LogGroups[2].GetLogGroupId())
	assert.Equal(t, "0|MTcyOTA3MDYxODQyODA0NDY1NA==", logGroupList.LogGroups[1].GetLogGroupId())
	assert.Equal(t, "0|MTcyOTA3MDYxODQyODA0NDY1Mw==", logGroupList.LogGroups[0].GetLogGroupId())
	empty := &LogGroup{}
	assert.Equal(t, empty.GetLogGroupId(), "")
	assert.Equal(t, "0|MTcyOTA3MDYxODQyODA0NDY1NA==|2|1", logGroupList.LogGroups[1].GetLogId(1))
	assert.Equal(t, "0|MTcyOTA3MDYxODQyODA0NDY1NQ==|3|0", logGroupList.LogGroups[2].GetLogId(0))
}
