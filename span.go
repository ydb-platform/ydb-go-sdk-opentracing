package ydb

import (
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
)

var (
	_ spans.Span = (*span)(nil)
	_ spans.Span = noopSpan{}
)

type (
	span struct {
		span opentracing.Span
	}
	noopSpan struct{}
)

func (noopSpan) ID() (_ string, valid bool) {
	return "", false
}

func (noopSpan) TraceID() (_ string, valid bool) {
	return "", false
}

func (noopSpan) Link(link spans.Span, attributes ...spans.KeyValue) {}

func (noopSpan) Log(msg string, attributes ...spans.KeyValue) {}

func (noopSpan) Warn(err error, attributes ...spans.KeyValue) {}

func (noopSpan) Error(err error, attributes ...spans.KeyValue) {}

func (noopSpan) End(attributes ...spans.KeyValue) {}

func (s *span) ID() (_ string, valid bool) {
	return "", false
}

func (s *span) Log(msg string, fields ...spans.KeyValue) {
	s.span.LogFields(append(
		fieldsToFields(fields),
		log.Event(msg),
	)...)
}

func (s *span) Warn(err error, fields ...spans.KeyValue) {
	s.span.LogFields(append(
		fieldsToFields(fields),
		log.Event(err.Error()),
	)...)
}

func (s *span) Error(err error, fields ...spans.KeyValue) {
	s.span.LogFields(append(
		fieldsToFields(fields),
		log.Error(err),
	)...)
}

func (s *span) TraceID() (string, bool) {
	return "", false
}

func (s *span) Link(link spans.Span, fields ...spans.KeyValue) {
	_ = opentracing.FollowsFrom(link.(*span).span.Context()) //nolint:forcetypeassert
}

func (s *span) End(fields ...spans.KeyValue) {
	s.span.FinishWithOptions(opentracing.FinishOptions{
		LogRecords: []opentracing.LogRecord{{
			Fields: fieldsToFields(fields),
		}},
	})
}
