package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"runtime"
	"strings"
)

// consoleHandler 只负责示例的终端格式，库仍使用标准 slog.Debug。
// 第一行显示源码位置，第二行缩进后显示时间、PID、级别、消息和业务字段。
type consoleHandler struct {
	output *log.Logger // log.Logger 一次写入完整日志，保证两行内容不会与其他日志交错。
	level  slog.Level
	pid    int
	attrs  []slog.Attr
}

func newConsoleHandler(w io.Writer, level slog.Level) slog.Handler {
	return &consoleHandler{output: log.New(w, "", 0), level: level, pid: os.Getpid()}
}

func (h *consoleHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *consoleHandler) Handle(_ context.Context, record slog.Record) error {
	var line strings.Builder
	var source string
	// PC 指向实际的 slog 调用位置，不能用 runtime.Caller 获取当前 Handler 的位置。
	if record.PC != 0 {
		frame, _ := runtime.CallersFrames([]uintptr{record.PC}).Next()
		if frame.File != "" {
			source = fmt.Sprintf("%s:%d", frame.File, frame.Line)
		}
	}
	if !record.Time.IsZero() {
		line.WriteString(record.Time.Format("2006/01/02 15:04:05"))
		line.WriteByte(' ')
	}
	fmt.Fprintf(&line, "[%d] %-5s ", h.pid, record.Level)
	line.WriteString(record.Message)
	var values []string
	for _, attr := range h.attrs {
		values = appendValues(values, attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		values = appendValues(values, attr)
		return true
	})
	if len(values) > 0 {
		fmt.Fprintf(&line, " %s ", strings.Join(values, ", "))
	}
	// 先转义正文中的换行，再插入格式换行，让正文始终保持在缩进的一行。
	text := strings.ReplaceAll(strings.ReplaceAll(line.String(), "\r", `\r`), "\n", `\n`)
	if source != "" {
		text = source + "\n    " + text
	}
	return h.output.Output(0, text)
}

func (h *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &clone
}

func (h *consoleHandler) WithGroup(string) slog.Handler {
	// 终端格式省略组名，分组不改变业务字段的输出顺序。
	return h
}

func appendValues(values []string, attr slog.Attr) []string {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) || (attr.Value.Kind() == slog.KindAny && attr.Value.Any() == nil) {
		return values // 省略空属性和 error=nil 等无内容字段。
	}
	if attr.Value.Kind() == slog.KindGroup {
		for _, child := range attr.Value.Group() {
			values = appendValues(values, child)
		}
		return values
	}
	return append(values, attr.Key+"="+attr.Value.String())
}
