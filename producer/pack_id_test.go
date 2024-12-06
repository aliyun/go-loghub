package producer

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackIdGenerator(t *testing.T) {
	g := newPackIdGenerator()
	wg := &sync.WaitGroup{}
	m := 1000
	n := 10
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			project := fmt.Sprintf("test%d", i)
			logstore := fmt.Sprintf("test%d", i)
			results := make([]string, 0, m)
			for j := 0; j < m; j++ {
				result := g.GeneratePackId(project, logstore)
				results = append(results, result)
			}
			prefix := results[0][:16]
			for j := 0; j < m; j++ {
				assert.Equal(t, prefix, results[j][:16])
				suffix := results[j][17:]
				assert.Equal(t, fmt.Sprintf("%X", j), suffix)
			}

			wg.Done()
		}(i)
	}
	wg.Wait()
}

// BenchmarkPackIdGenerator-12    	 8366719	       120.7 ns/op	      64 B/op	       4 allocs/op
func BenchmarkPackIdGenerator(b *testing.B) {
	g := newPackIdGenerator()
	for i := 0; i < b.N; i++ {
		g.GeneratePackId("test", "test")
	}
}
