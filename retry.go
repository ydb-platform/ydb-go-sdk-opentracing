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
		t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_retry",
			)
			if info.NestedCall {
				start.SetTag(string(ext.Error), true)
				start.LogFields(otlog.Error(fmt.Errorf("nested call")))
			}
			start.SetBaggageItem("idempotent", str.Bool(info.Idempotent))
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.RetryLoopDoneInfo) {
					finish(start,
						info.Error,
						otlog.Int("attempts", info.Attempts),
					)
				}
			}
		}
	}
	return t
}
