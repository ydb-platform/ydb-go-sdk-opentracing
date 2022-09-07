package main

import (
	"context"
	"database/sql"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	jaegerConfig "github.com/uber/jaeger-client-go/config"

	ydbTracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

const (
	tracerURL   = "localhost:5775"
	serviceName = "bench"
	prefix      = "ydb-go-sdk-opentracing/bench/database-sql"
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
}

func main() {
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

	defer closer.Close()

	// set global tracer of this application
	opentracing.SetGlobalTracer(tracer)

	span, ctx := opentracing.StartSpanFromContext(context.Background(), "client")
	defer span.Finish()

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDiscoveryInterval(5*time.Second),
		ydbTracing.WithTraces(trace.DetailsAll),
	)
	if err != nil {
		log.Fatalf("connect error: %v", err)
	}
	defer func() { _ = nativeDriver.Close(ctx) }()

	connector, err := ydb.Connector(nativeDriver)
	if err != nil {
		log.Fatalf("create connector failed: %v", err)
	}

	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cc, err := ydb.Unwrap(db)
	if err != nil {
		log.Fatalf("unwrap failed: %v", err)
	}

	prefix := path.Join(cc.Name(), prefix)

	err = sugar.RemoveRecursive(ctx, cc, prefix)
	if err != nil {
		log.Fatalf("remove recursive failed: %v", err)
	}

	err = prepareSchema(ctx, db, prefix)
	if err != nil {
		log.Fatalf("create tables error: %v", err)
	}

	err = fillTablesWithData(ctx, db, prefix)
	if err != nil {
		log.Fatalf("fill tables with data error: %v", err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for {
				err = fillTablesWithData(ctx, db, prefix)
				if err != nil {
					log.Fatalf("fill tables with data error: %v", err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				err = selectDefault(ctx, db, prefix)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				err = selectScan(ctx, db, prefix)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()
}
