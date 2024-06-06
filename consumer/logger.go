package consumerLibrary

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ZapLoggerAdapter struct {
	logger *zap.Logger
	levels map[interface{}]zapcore.Level
}

func newZapLoggerAdapter(logger *zap.Logger) log.Logger {
	levels := map[interface{}]zapcore.Level{
		level.DebugValue(): zapcore.DebugLevel,
		level.InfoValue():  zapcore.InfoLevel,
		level.WarnValue():  zapcore.WarnLevel,
		level.ErrorValue(): zapcore.ErrorLevel,
	}
	return &ZapLoggerAdapter{logger: logger, levels: levels}
}

func (adapter *ZapLoggerAdapter) Level(gokitLevel interface{}) zapcore.Level {
	level, ok := adapter.levels[gokitLevel]
	if !ok {
		return zapcore.InfoLevel
	}
	return level
}

func (adapter *ZapLoggerAdapter) Log(keyvals ...interface{}) error {
	var msg string
	var lvl zapcore.Level
	fields := make([]zap.Field, 0)
	for i := 0; i < len(keyvals); i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			continue
		}
		switch key {
		case "level":
			lvl = adapter.Level(keyvals[i+1])
		case "msg":
			msg, ok = keyvals[i+1].(string)
			if !ok {
				msg = ""
			}
		default:
			fields = append(fields, zap.Any(key, keyvals[i+1]))
		}
	}
	adapter.logger.Log(lvl, msg, fields...)
	return nil
}
