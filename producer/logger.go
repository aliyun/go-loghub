package producer

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"gopkg.in/natefinch/lumberjack.v2"

	"os"
)

func logConfig(producerConfig *ProducerConfig) log.Logger {
	var logger log.Logger

	if producerConfig.LogFileName == "" {
		if producerConfig.IsJsonType {
			logger = log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
		} else {
			logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
		}
	} else {
		if producerConfig.IsJsonType {
			logger = log.NewLogfmtLogger(initLogFlusher(producerConfig))
		} else {
			logger = log.NewJSONLogger(initLogFlusher(producerConfig))
		}
	}
	switch producerConfig.AllowLogLevel {
	case "debug":
		logger = level.NewFilter(logger, level.AllowDebug())
	case "info":
		logger = level.NewFilter(logger, level.AllowInfo())
	case "warn":
		logger = level.NewFilter(logger, level.AllowWarn())
	case "error":
		logger = level.NewFilter(logger, level.AllowError())
	default:
		logger = level.NewFilter(logger, level.AllowInfo())
	}
	logger = log.With(logger, "time", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	return logger
}

func initLogFlusher(producerConfig *ProducerConfig) *lumberjack.Logger {
	if producerConfig.LogMaxSize == 0 {
		producerConfig.LogMaxSize = 10
	}
	if producerConfig.LogMaxBackups == 0 {
		producerConfig.LogMaxBackups = 10
	}
	return &lumberjack.Logger{
		Filename:   producerConfig.LogFileName,
		MaxSize:    producerConfig.LogMaxSize,
		MaxBackups: producerConfig.LogMaxBackups,
		Compress:   producerConfig.LogCompress,
	}
}
