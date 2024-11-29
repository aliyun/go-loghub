package sls

import (
	"github.com/klauspost/compress/zstd"
)

var slsZstdCompressor LogCompressor = NewZstdCompressor(zstd.SpeedFastest)

func SetZstdCompressor(compressor LogCompressor) error {
	slsZstdCompressor = compressor
	return nil
}

type LogCompressor interface {
	// Compress src into dst.  If you have a buffer to use, you can pass it to
	// prevent allocation.  If it is too small, or if nil is passed, a new buffer
	// will be allocated and returned.
	Compress(src, dst []byte) ([]byte, error)
	// Decompress src into dst.  If you have a buffer to use, you can pass it to
	// prevent allocation.  If it is too small, or if nil is passed, a new buffer
	// will be allocated and returned.
	Decompress(src, dst []byte) ([]byte, error)
}

type ZstdCompressor struct {
	writer *zstd.Encoder
	reader *zstd.Decoder
	level  zstd.EncoderLevel
}

func NewZstdCompressor(level zstd.EncoderLevel) *ZstdCompressor {
	res := &ZstdCompressor{
		level: level,
	}
	res.writer, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(res.level))
	res.reader, _ = zstd.NewReader(nil)
	return res
}

func (c *ZstdCompressor) Compress(src, dst []byte) ([]byte, error) {
	if dst != nil {
		return c.writer.EncodeAll(src, dst[:0]), nil
	}
	return c.writer.EncodeAll(src, nil), nil
}

func (c *ZstdCompressor) Decompress(src, dst []byte) ([]byte, error) {
	if dst != nil {
		return c.reader.DecodeAll(src, dst[:0])
	}
	return c.reader.DecodeAll(src, nil)
}
