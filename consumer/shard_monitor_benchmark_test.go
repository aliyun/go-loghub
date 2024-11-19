package consumerLibrary

import (
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

// BenchmarkRecordFetchRequest
// BenchmarkRecordFetchRequest-12    	29816072	        40.05 ns/op	       0 B/op	       0 allocs/op
func BenchmarkRecordFetchRequest(b *testing.B) {
	shardMonitor := newShardMonitor(1, time.Second)
	start := time.Now()
	plm := &sls.PullLogMeta{RawSize: 1}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		shardMonitor.RecordFetchRequest(plm, nil, start)
	}
}

// BenchmarkRecordProcess
// BenchmarkRecordProcess-12         	33092797	        35.15 ns/op	       0 B/op	       0 allocs/op
func BenchmarkRecordProcess(b *testing.B) {
	shardMonitor := newShardMonitor(1, time.Second)
	start := time.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		shardMonitor.RecordProcess(nil, start)
	}
}
