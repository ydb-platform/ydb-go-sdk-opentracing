module github.com/ydb-platform/ydb-go-sdk-opentracing/internal/cmd/bench

go 1.17

require (
	github.com/opentracing/opentracing-go v1.2.0
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/ydb-platform/ydb-go-sdk-opentracing v0.0.5
	github.com/ydb-platform/ydb-go-sdk/v3 v3.7.1
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/golang/protobuf v1.5.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20211103074319-526e57659e16 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859 // indirect
	golang.org/x/sys v0.0.0-20190412213103-97732733099d // indirect
	golang.org/x/text v0.3.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	google.golang.org/grpc v1.38.0 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
)

replace github.com/ydb-platform/ydb-go-sdk-opentracing => ../../../
