module github.com/ydb-platform/ydb-go-sdk-opentracing

go 1.16

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1 // indirect
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/ydb-platform/ydb-go-sdk/v3 v3.4.2
	go.uber.org/atomic v1.9.0 // indirect
)

replace github.com/ydb-platform/ydb-go-sdk/v3 => ../ydb-go-sdk-private
