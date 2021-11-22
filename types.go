package tracing

import (
	"context"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"net/url"

	"github.com/opentracing/opentracing-go"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type option func(holder *options)

func WithDetails(details trace.Details) option {
	return func(c *options) {
		c.details |= details
	}
}

type options struct {
	details trace.Details
}

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

func startSpan(ctx *context.Context, operationName string, alternatingKeyValues ...interface{}) (s opentracing.Span) {
	var childCtx context.Context
	s, childCtx = opentracing.StartSpanFromContext(*ctx, operationName)
	*ctx = childCtx
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
