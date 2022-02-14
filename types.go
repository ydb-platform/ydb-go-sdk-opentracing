package tracing

import (
	"context"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"net/url"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
)

func logError(s opentracing.Span, err error) {
	s.SetTag(string(ext.Error), true)
	s.LogFields(log.Error(err))
	m := retry.Check(err)
	s.SetTag(string(ext.Error)+".retryable", m.MustRetry(false))
	s.SetTag(string(ext.Error)+".delete_session", m.MustDeleteSession())
	s.SetTag(string(ext.Error)+".backoff", m.BackoffType().String())
}

func finish(s opentracing.Span, err error, alternatingKeyValues ...interface{}) {
	if err != nil {
		logError(s, err)
	}
	s.LogKV(alternatingKeyValues...)
	s.Finish()
}

func intermediate(s opentracing.Span, err error, alternatingKeyValues ...interface{}) {
	if err != nil {
		logError(s, err)
	}
	s.LogKV(alternatingKeyValues...)
}

type counter struct {
	span    opentracing.Span
	counter int64
	name    string
}

func (s *counter) add(delta int64) {
	atomic.AddInt64(&s.counter, delta)
	s.span.LogKV(s.name, atomic.LoadInt64(&s.counter))
}

func startSpanWithCounter(ctx *context.Context, operationName string, counterName string, alternatingKeyValues ...interface{}) (c *counter) {
	defer func() {
		c.span.SetTag("ydb.driver.sensor", operationName+"_"+counterName)
	}()
	return &counter{
		span:    startSpan(ctx, operationName, alternatingKeyValues...),
		counter: 0,
		name:    counterName,
	}
}

func startSpan(ctx *context.Context, operationName string, alternatingKeyValues ...interface{}) (s opentracing.Span) {
	if ctx != nil {
		var childCtx context.Context
		s, childCtx = opentracing.StartSpanFromContext(*ctx, operationName)
		*ctx = childCtx
	} else {
		s = opentracing.StartSpan(operationName)
	}
	s.SetTag("scope", "ydb")
	s.SetTag("version", ydb.Version)
	s.LogKV(alternatingKeyValues...)
	return s
}

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		return ""
	}
	return u.Query().Get("node_id")
}
