package ydb

import (
	"github.com/opentracing/opentracing-go"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(c *adapter)

func WithTracer(tracer opentracing.Tracer) Option {
	return func(c *adapter) {
		c.tracer = tracer
	}
}

func WithDetailer(d trace.Detailer) Option {
	return func(c *adapter) {
		c.detailer = d
	}
}
