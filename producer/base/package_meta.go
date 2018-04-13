package log_producer

import (
	"log"
	"sync"
	"time"
)

type PackageMeta struct {
	Name           string
	ArriveTimeInMS int64
	LogLinesCount  int
	PackageBytes   int
	Lock           *sync.Mutex
}

func (p *PackageMeta) Clear() {
	p.ArriveTimeInMS = time.Now().UnixNano() / (1000 * 1000)
	p.LogLinesCount = 0
	p.PackageBytes = 0

	log.Println("clear PackageMeta")
}
