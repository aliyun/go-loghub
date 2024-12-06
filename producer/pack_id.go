package producer

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type PackIdGenerator struct {
	mutex                   sync.RWMutex
	logstorePackIdGenerator map[string]*LogStorePackIdGenerator
	count                   atomic.Int32
}

func newPackIdGenerator() *PackIdGenerator {
	return &PackIdGenerator{
		logstorePackIdGenerator: make(map[string]*LogStorePackIdGenerator),
	}
}

func (g *PackIdGenerator) GeneratePackId(project, logstore string) string {
	key := project + "|" + logstore

	// fast path, logstore already has a generator
	g.mutex.RLock()
	if l, ok := g.logstorePackIdGenerator[key]; ok {
		packNumber := l.packNumber.Add(1)
		g.mutex.RUnlock()
		return fmt.Sprintf("%s%X", l.prefix, packNumber-1)
	}
	g.mutex.RUnlock()

	// slow path
	g.mutex.Lock()
	if _, ok := g.logstorePackIdGenerator[key]; !ok {
		g.logstorePackIdGenerator[key] = newLogStorePackIdGenerator(g.count.Add(1))
	}
	l := g.logstorePackIdGenerator[key]
	packNumber := l.packNumber.Add(1)
	g.mutex.Unlock()
	return fmt.Sprintf("%s%X", l.prefix, packNumber-1)
}

type LogStorePackIdGenerator struct {
	packNumber atomic.Int64
	prefix     string // with "-"
}

func newLogStorePackIdGenerator(id int32) *LogStorePackIdGenerator {
	hash := fmt.Sprintf("%d-%d", time.Now().UnixNano(), id)
	return &LogStorePackIdGenerator{
		packNumber: atomic.Int64{},
		prefix:     strings.ToUpper(generatePackId(hash)) + "-",
	}
}
