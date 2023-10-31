package tracing

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"net/url"
)

func logError(s opentracing.Span, err error) {
	s.SetTag(string(ext.Error), true)
	s.LogFields(otlog.Error(err))
	m := retry.Check(err)
	if v := s.BaggageItem("idempotent"); v != "" {
		s.SetTag(string(ext.Error)+".retryable", m.MustRetry(v == "true"))
	}
	s.SetTag(string(ext.Error)+".delete_session", m.MustDeleteSession())
}

func finish(s opentracing.Span, err error, fields ...otlog.Field) {
	if err != nil {
		logError(s, err)
	}
	s.LogFields(fields...)
	s.Finish()
}

type counter struct {
	span    opentracing.Span
	counter int64
	name    string
}

func startSpanWithCounter(ctx *context.Context, operationName string, counterName string, fields ...otlog.Field) (c *counter) {
	defer func() {
		c.span.SetTag("ydb.driver.sensor", operationName+"_"+counterName)
	}()
	return &counter{
		span:    startSpan(ctx, operationName, fields...),
		counter: 0,
		name:    counterName,
	}
}

func startSpan(ctx *context.Context, operationName string, fields ...otlog.Field) (s opentracing.Span) {
	if ctx != nil {
		var childCtx context.Context
		s, childCtx = opentracing.StartSpanFromContext(*ctx, operationName)
		*ctx = childCtx
	} else {
		s = opentracing.StartSpan(operationName)
	}
	s.SetTag("ydb-go-sdk", "v"+ydb.Version)
	s.LogFields(fields...)
	return s
}

func followSpan(
	related opentracing.SpanContext,
	operationName string,
	fields ...otlog.Field,
) (s opentracing.Span) {
	s = opentracing.StartSpan(operationName, opentracing.FollowsFrom(related))
	s.SetTag("ydb-go-sdk", "v"+ydb.Version)
	s.LogFields(fields...)
	return s
}

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		return ""
	}
	return u.Query().Get("node_id")
}
