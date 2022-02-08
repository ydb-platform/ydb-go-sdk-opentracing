module github.com/ydb-platform/ydb-go-sdk-opentracing/internal/cmd/bench

go 1.17

require (
	github.com/opentracing/opentracing-go v1.2.0
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/ydb-platform/ydb-go-sdk-opentracing v0.0.10
	github.com/ydb-platform/ydb-go-sdk/v3 v3.9.0
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/golang/protobuf v1.5.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20220203104745-929cf9c248bc // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/sys v0.0.0-20200323222414-85ca7c5b95cd // indirect
	golang.org/x/text v0.3.0 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	google.golang.org/grpc v1.43.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)

replace github.com/ydb-platform/ydb-go-sdk-opentracing => ../../../
