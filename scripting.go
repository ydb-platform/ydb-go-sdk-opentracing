package tracing

import (
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/safe"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Scripting(details trace.Details) (t trace.Scripting) {
	if details&trace.ScriptingEvents != 0 {
		t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_scripting_execute",
				otlog.String("query", info.Query),
				otlog.String("params", safe.Stringer(info.Parameters)),
			)
			return func(info trace.ScriptingExecuteDoneInfo) {
				if info.Error == nil {
					finish(
						start,
						safe.Err(info.Result),
					)
				} else {
					finish(
						start,
						info.Error,
					)
				}
			}
		}
		t.OnStreamExecute = func(
			info trace.ScriptingStreamExecuteStartInfo,
		) func(
			trace.ScriptingStreamExecuteIntermediateInfo,
		) func(
			trace.ScriptingStreamExecuteDoneInfo,
		) {
			start := startSpan(
				info.Context,
				"ydb_scripting_stream_execute",
				otlog.String("query", info.Query),
				otlog.String("params", safe.Stringer(info.Parameters)),
			)
			start.Finish()
			return func(
				info trace.ScriptingStreamExecuteIntermediateInfo,
			) func(
				trace.ScriptingStreamExecuteDoneInfo,
			) {
				s := followSpan(start.Context(), "ydb_scripting_stream_execute_recv")
				if info.Error != nil {
					logError(s, info.Error)
				}
				s.Finish()
				return func(info trace.ScriptingStreamExecuteDoneInfo) {
					s := followSpan(start.Context(), "ydb_scripting_stream_execute_done")
					if info.Error != nil {
						logError(s, info.Error)
					}
					s.Finish()
				}
			}
		}
		t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_scripting_explain",
				otlog.String("query", info.Query),
			)
			return func(info trace.ScriptingExplainDoneInfo) {
				finish(start, info.Error)
			}
		}
		t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_scripting_close",
			)
			return func(info trace.ScriptingCloseDoneInfo) {
				finish(start, info.Error)
			}
		}
	}
	return t
}
