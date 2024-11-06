package ydb

import (
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
)

var (
	_ spans.Span = (*span)(nil)
	_ spans.Span = nopSpan{}
)

type (
	span struct {
		span opentracing.Span
	}
	nopSpan struct{}
)

func (nopSpan) ID() (_ string, valid bool) {
	return "", false
}

func (nopSpan) TraceID() (_ string, valid bool) {
	return "", false
}

func (nopSpan) Link(link spans.Span, attributes ...spans.KeyValue) {}

func (nopSpan) Log(msg string, attributes ...spans.KeyValue) {}

func (nopSpan) Warn(err error, attributes ...spans.KeyValue) {}

func (nopSpan) Error(err error, attributes ...spans.KeyValue) {}

func (nopSpan) End(attributes ...spans.KeyValue) {}

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
