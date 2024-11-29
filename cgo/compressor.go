package cgo

import (
	"sync"

	"github.com/DataDog/zstd"
	sls "github.com/aliyun/aliyun-log-go-sdk"
)

func SetZstdCgoCompressor(compressLevel int) error {
	sls.SetZstdCompressor(newZstdCompressor(compressLevel))
	return nil
}

type zstdCompressor struct {
	ctxPool sync.Pool
	level   int
}

func newZstdCompressor(level int) *zstdCompressor {
	res := &zstdCompressor{
		level: level,
	}
	res.ctxPool = sync.Pool{
		New: func() interface{} {
			return zstd.NewCtx()
		},
	}
	return res
}

func (c *zstdCompressor) Compress(src, dst []byte) ([]byte, error) {
	zstdCtx := c.ctxPool.Get().(zstd.Ctx)
	defer c.ctxPool.Put(zstdCtx)
	return zstdCtx.CompressLevel(dst, src, c.level)
}

func (c *zstdCompressor) Decompress(src, dst []byte) ([]byte, error) {
	zstdCtx := c.ctxPool.Get().(zstd.Ctx)
	defer c.ctxPool.Put(zstdCtx)
	return zstdCtx.Decompress(dst, src)
}
