package debugutils

import (
	"fmt"
	"time"
)

const MSG_CHAN_BUFFER_SIZE = 0

var printMsgChan chan string

func init() {
	printMsgChan = make(chan string, MSG_CHAN_BUFFER_SIZE)
	go func() {
		for {
			msg := <-printMsgChan
			fmt.Println(msg)
		}
	}()
}

type Logger struct {
	name     string
	logLevel LogLevel
	msgChan  chan string
}

type LogLevel int

// 日志等级
const (
	Slient = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// 默认日志等级
const DEFAULT_LOG_LEVEL = ErrorLevel

var logLevelName = map[LogLevel]string{
	Slient:     "SLIENT",
	InfoLevel:  "INFO",
	WarnLevel:  "WARN",
	ErrorLevel: "ERROR",
}

// 输出色彩
const (
	// BaseFormat  = "\033[1;34m%s\033[0m"
	// InfoFormat  = "\033[1;32m%s\033[0m"
	// WarnFormat  = "\033[1;33m%s\033[0m"
	// ErrorFormat = "\033[1;31m%s\033[0m"
	BaseFormat  = "%s"
	InfoFormat  = "%s"
	WarnFormat  = "%s"
	ErrorFormat = "%s"
)

func NewLogger(name string) *Logger {
	logger := Logger{
		name:     name,
		logLevel: DEFAULT_LOG_LEVEL,
		msgChan:  printMsgChan,
	}
	return &logger
}

func (logger *Logger) SetLogLevel(level LogLevel) {
	logger.logLevel = level
	logger.Printf("Set log level to \"%s\"\n", logLevelName[logger.logLevel])
}

func (logger *Logger) Printf(format string, v ...any) {
	content := fmt.Sprintf(format, v...)
	fmt.Printf(BaseFormat, content)
}

const timeFormat = "2006/01/02 15:04:05.000"

func (logger *Logger) getPrefix() string {
	now := time.Now().Format(timeFormat)
	return fmt.Sprintf("%s [%s]: ", now, logger.name)
}

func (logger *Logger) Info(format string, v ...any) {
	if logger.logLevel >= InfoLevel {
		// 使用传递和使用参数要使用v...
		content := fmt.Sprintf("[%5s] %s"+format,
			append([]interface{}{logLevelName[InfoLevel], logger.getPrefix()}, v...)...)
		msg := fmt.Sprintf(InfoFormat, content)
		logger.msgChan <- msg
	}
}

func (logger *Logger) Warn(format string, v ...any) {
	if logger.logLevel >= WarnLevel {
		content := fmt.Sprintf("[%5s] %s"+format,
			append([]interface{}{logLevelName[WarnLevel], logger.getPrefix()}, v...)...)
		msg := fmt.Sprintf(WarnFormat, content)
		logger.msgChan <- msg
	}
}

func (logger *Logger) Error(format string, v ...any) {
	if logger.logLevel >= ErrorLevel {
		content := fmt.Sprintf("[%5s] %s"+format,
			append([]interface{}{logLevelName[ErrorLevel], logger.getPrefix()}, v...)...)
		msg := fmt.Sprintf(ErrorFormat, content)
		logger.msgChan <- msg
	}
}
