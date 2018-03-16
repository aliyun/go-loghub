package sls_producer

import (
	"fmt"
	"sync"
)

type PackageMeta struct {
	Name           string
	ArriveTimeInMS int64
	LogLinesCount  int
	PackageBytes   int
	Lock           *sync.Mutex
}

func (p *PackageMeta) Clear() {
	p.ArriveTimeInMS = 0
	p.LogLinesCount = 0
	p.PackageBytes = 0

	fmt.Println("clear PackageMeta")
}
