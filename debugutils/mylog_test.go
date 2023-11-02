package debugutils

import (
	"testing"
)

var logger = NewLogger("S0", -1)

func init() {
	logger.SetLogLevel(ErrorLevel)
}

func TestInfo(t *testing.T) {
	logger.Info("test INFO %d", 1)
}

func TestWarn(t *testing.T) {
	logger.Warn("test WARN %d", 2)
}

func TestError(t *testing.T) {
	logger.Error("test ERROR %d", 3)
}
