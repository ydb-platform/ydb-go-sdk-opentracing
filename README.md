# tracing

tracing package helps to create ydb-go-sdk opentracing traces 

## Usage
```go
import (
    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    tracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
    jaegerConfig "github.com/uber/jaeger-client-go/config"
)

...
    // init jaeger client
    tracer, closer, err := jaegerConfig.Configuration{
        ServiceName: serviceName,
        Sampler: &jaegerConfig.SamplerConfig{
            Type:  "const",
            Param: 1,
        },
        Reporter: &jaegerConfig.ReporterConfig{
            LogSpans:            true,
            BufferFlushInterval: 1 * time.Second,
            LocalAgentHostPort:  tracerURL,
        },
    }.NewTracer()
    if err != nil {
        panic(err)
    }

    db, err := ydb.New(
        ctx,
        ydb.MustConnectionString(connection),
        ydb.WithTraceDriver(tracing.Driver(
            tracing.WithDetails(trace.DetailsAll),
        )),
        ydb.WithTraceTable(tracing.Table(
            tracing.WithDetails(trace.DetailsAll),
        )),
    )

```
