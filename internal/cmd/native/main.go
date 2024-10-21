package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	jaegerConfig "github.com/uber/jaeger-client-go/config"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	tracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
)

func initJaeger(service string) (opentracing.Tracer, io.Closer) {
	cfg := &jaegerConfig.Configuration{
		ServiceName: service,
		Sampler: &jaegerConfig.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &jaegerConfig.ReporterConfig{
			LogSpans: true,
		},
	}
	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	return tracer, closer
}

func main() {
	tracer, closer := initJaeger("test")
	defer closer.Close()
	opentracing.SetGlobalTracer(tracer)

	span, ctx := opentracing.StartSpanFromContext(context.Background(), "client")
	defer span.Finish()

	var creds ydb.Option
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		creds = ydb.WithAccessTokenCredentials(token)
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		creds = ydb.WithAnonymousCredentials()
	}
	db, err := ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithBalancer(balancers.RandomChoice()),
		creds,
		ydb.WithSessionPoolSizeLimit(300),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		tracing.WithTraces(
			tracing.WithTracer(tracer),
			tracing.WithDetailer(trace.DetailsAll),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	_ = upsertData(ctx, db.Table(), db.Name(), "series", 100)

	concurrency := func() int {
		if concurrency, err := strconv.Atoi(os.Getenv("CONCURRENCY")); err != nil && concurrency > 0 {
			return concurrency
		}
		return 300
	}()

	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
				_, _ = scanSelect(
					ctx,
					db.Table(),
					db.Name(),
					rand.Int63n(25000),
				)
			}
		}()
	}
	wg.Wait()
}

func upsertData(ctx context.Context, c table.Client, prefix, tableName string, concurrency int) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(prefix, tableName))
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return err
	}
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(prefix, tableName),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeDate)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return err
	}
	rowsLen := 25000000
	batchSize := 1000
	wg := sync.WaitGroup{}
	sema := make(chan struct{}, concurrency)
	for shift := 0; shift < rowsLen; shift += batchSize {
		wg.Add(1)
		sema <- struct{}{}
		go func(prefix, tableName string, shift int) {
			defer func() {
				<-sema
				wg.Done()
			}()
			rows := make([]types.Value, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("series_id", types.Uint64Value(uint64(i+shift+3))),
					types.StructFieldValue("title", types.UTF8Value(fmt.Sprintf("series No. %d title", i+shift+3))),
					types.StructFieldValue("series_info", types.UTF8Value(fmt.Sprintf("series No. %d info", i+shift+3))),
					types.StructFieldValue("release_date", types.DateValueFromTime(time.Now())),
					types.StructFieldValue("comment", types.UTF8Value(fmt.Sprintf("series No. %d comment", i+shift+3))),
				))
			}
			_ = c.Do(ctx,
				func(ctx context.Context, session table.Session) (err error) {
					return session.BulkUpsert(
						ctx,
						path.Join(prefix, tableName),
						types.ListValue(rows...),
					)
				},
				table.WithIdempotent(),
			)
		}(prefix, tableName, shift)
	}
	wg.Wait()
	return nil
}

func scanSelect(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");
		SELECT
			series_id,
			title,
			release_date
		FROM series LIMIT %d;`,
		prefix,
		limit,
	)
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			var res result.StreamResult
			count = 0
			res, err = s.StreamExecuteScanQuery(
				ctx,
				query,
				table.NewQueryParameters(),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			var (
				id    *uint64
				title *string
				date  *time.Time
			)
			log.Printf("> select_simple_transaction:\n")
			for res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					count++
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
					log.Printf(
						"  > %d %s %s\n",
						*id, *title, *date,
					)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
	)
	return
}
