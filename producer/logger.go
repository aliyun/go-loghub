package producer

import (
	"io"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"gopkg.in/natefinch/lumberjack.v2"
)

func getProducerLogger(producerConfig *ProducerConfig) log.Logger {
	// if producerConfig.Logger is not nil, use it as the logger
	// and ignore AllowLogLevel/LogFileName/LogMaxSize/LogMaxBackups/LogCompass
	if producerConfig.Logger != nil {
		return producerConfig.Logger
	}
	var writer io.Writer
	if producerConfig.LogFileName == "" {
		writer = log.NewSyncWriter(os.Stdout)
	} else {
		if producerConfig.LogMaxSize == 0 {
			producerConfig.LogMaxSize = 10
		}
		if producerConfig.LogMaxBackups == 0 {
			producerConfig.LogMaxBackups = 10
		}
		writer = &lumberjack.Logger{
			Filename:   producerConfig.LogFileName,
			MaxSize:    producerConfig.LogMaxSize,
			MaxBackups: producerConfig.LogMaxBackups,
			Compress:   producerConfig.LogCompress,
		}
	}
	var logger log.Logger
	if producerConfig.IsJsonType {
		logger = log.NewJSONLogger(writer)
	} else {
		logger = log.NewLogfmtLogger(writer)
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
