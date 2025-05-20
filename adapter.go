package ydb

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ spans.Adapter = (*adapter)(nil)

type adapter struct {
	tracer   opentracing.Tracer
	detailer trace.Detailer
}

func (cfg *adapter) Details() trace.Details {
	return cfg.detailer.Details()
}

func (cfg *adapter) SpanFromContext(ctx context.Context) spans.Span {
	s := opentracing.SpanFromContext(ctx)
	
	if s == nil {
		return noopSpan{}
	}

	return &span{
		span: s,
	}
}

func (cfg *adapter) Start(ctx context.Context, operationName string, fields ...spans.KeyValue) (
	context.Context, spans.Span,
) {
	tags := opentracing.Tags(make(map[string]interface{}))
	for _, kv := range fieldsToFields(fields) {
		tags[kv.Key()] = kv.Value()
	}
	s, childCtx := opentracing.StartSpanFromContextWithTracer(ctx, cfg.tracer, operationName, tags)

	return childCtx, &span{
		span: s,
	}
}

func WithTraces(opts ...Option) ydb.Option {
	cfg := &adapter{
		detailer: trace.DetailsAll,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.tracer == nil {
		cfg.tracer = opentracing.GlobalTracer()
	}

	return spans.WithTraces(cfg)
}
