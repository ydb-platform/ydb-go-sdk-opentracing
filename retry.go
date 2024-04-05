package tracing

import (
	"fmt"
	"github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/str"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(details trace.Details) (t trace.Retry) {
	if details&trace.RetryEvents != 0 {
		t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_retry",
			)
			if info.NestedCall {
				start.SetTag(string(ext.Error), true)
				start.LogFields(otlog.Error(fmt.Errorf("nested call")))
			}
			start.SetBaggageItem("idempotent", str.Bool(info.Idempotent))
			start.Finish()
			return func(info trace.RetryLoopDoneInfo) {
				s := followSpan(start.Context(), "ydb_retry_done",
					otlog.Int("attempts", info.Attempts),
				)
				if info.Error != nil {
					logError(s, info.Error)
				}
				s.Finish()
			}
		}
	}
	return t
}
