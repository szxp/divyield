package logger

type Logger interface {
	Logf(format string, v ...interface{})
}
