package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/spf13/viper"
	jaegerConfig "github.com/uber/jaeger-client-go/config"
	env "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	tracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"io"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"path"
	"sync"
	"time"
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
	viper.AutomaticEnv()
}

func testQuery(ctx context.Context, db *ydb.Driver) error {
	const query = `SELECT 42 as id, "myStr" as myStr;`

	err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, _, err = s.Execute(ctx, table.DefaultTxControl(), query, nil)
		return err
	})
	if err != nil {
		return fmt.Errorf("testQuery: %w", err)
	}

	return nil
}

func initTracer(v *viper.Viper) (io.Closer, error) {
	tracer, closer, err := jaegerConfig.Configuration{
		ServiceName: v.GetString(ServiceName),
		Sampler: &jaegerConfig.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &jaegerConfig.ReporterConfig{
			LogSpans:            true,
			BufferFlushInterval: 1 * time.Second,
			CollectorEndpoint:   v.GetString(JaegerEndpoint),
		},
	}.NewTracer()
	if err != nil {
		return nil, err
	}
	opentracing.SetGlobalTracer(tracer)

	return closer, nil
}

func initConnectionToYdb(ctx context.Context, v *viper.Viper) (_ *ydb.Driver, closer io.Closer, err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "initialize connection to YDB")
	defer func() {
		if err != nil {
			span.SetTag("error", true)
		} else {
			span.SetTag("error", false)
		}
		span.Finish()
	}()

	db, err := ydb.Open(
		ctx,
		v.GetString(YdbConnectionString),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithBalancer(balancers.RandomChoice()),
		env.WithEnvironCredentials(ctx),
		ydb.WithSessionPoolSizeLimit(300),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		tracing.WithTraces(trace.DetailsAll),
	)
	if err != nil {
		return nil, nil, err
	}
	log.Println("connected to ydb")

	err = testQuery(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	log.Println("test query done")

	return db, closer, nil
}

func main() {
	v := initViper()
	closer, err := initTracer(v)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = closer.Close()
	}()

	span, ctx := opentracing.StartSpanFromContext(context.Background(), "client")
	defer span.Finish()

	db, closer, err := initConnectionToYdb(ctx, v)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = closer.Close()
	}()

	if v.GetBool(PrepareBenchData) {
		var tableName = "series"

		err = prepareTable(ctx, db.Table(), db.Name(), tableName)
		if err != nil {
			log.Fatal(err)
		}

		err = upsertData(ctx, v, db.Table(), db.Name(), tableName)
		if err != nil {
			log.Fatal(err)
		}

	}

	errCh := scanSelect(ctx, v, db.Table(), db.Name())
	for err := range errCh {
		log.Println(err)
	}
}

func prepareTable(ctx context.Context, c table.Client, prefix, tableName string) (err error) {
	log.Println("dropping table", path.Join(prefix, tableName))

	span, ctx := opentracing.StartSpanFromContext(ctx, "prepare table")
	defer func() {
		if err != nil {
			span.SetTag("error", true)
		} else {
			span.SetTag("error", false)
		}
		span.Finish()
	}()

	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(prefix, tableName))
		},
		table.WithIdempotent(),
	)
	if err != nil {
		// don't return error because operation is not idempotent
		log.Println("warning: error with dropping table: ", err)
	} else {
		log.Println("dropped table", path.Join(prefix, tableName))
	}

	log.Println("creating table", path.Join(prefix, tableName))
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
	)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	log.Println("created table", path.Join(prefix, tableName))
	return nil
}

func upsertData(ctx context.Context, v *viper.Viper, c table.Client, prefix, tableName string) (err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "upsert data")
	defer func() {
		if err != nil {
			span.SetTag("error", true)
		} else {
			span.SetTag("error", false)
		}
		span.Finish()
	}()

	var (
		wg    = sync.WaitGroup{}
		sema  = make(chan struct{}, v.GetInt(WorkersCount))
		errCh = make(chan error, v.GetInt(WorkersCount))
	)

	for shift := 0; shift < v.GetInt(RowsLen); shift += v.GetInt(BatchSize) {
		wg.Add(1)
		sema <- struct{}{}
		go func(prefix, tableName string, shift int) {
			log.Println("upserting with shift", shift)
			defer func() {
				log.Println("finished upserting with shift", shift)
				<-sema
				wg.Done()
			}()
			rows := make([]types.Value, 0, v.GetInt(BatchSize))
			for i := 0; i < v.GetInt(BatchSize); i++ {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("series_id", types.Uint64Value(uint64(i+shift+3))),
					types.StructFieldValue("title", types.UTF8Value(fmt.Sprintf("series No. %d title", i+shift+3))),
					types.StructFieldValue("series_info", types.UTF8Value(fmt.Sprintf("series No. %d info", i+shift+3))),
					types.StructFieldValue("release_date", types.DateValueFromTime(time.Now())),
					types.StructFieldValue("comment", types.UTF8Value(fmt.Sprintf("series No. %d comment", i+shift+3))),
				))
			}
			err = c.Do(ctx,
				func(ctx context.Context, session table.Session) (err error) {
					return session.BulkUpsert(
						ctx,
						path.Join(prefix, tableName),
						types.ListValue(rows...),
					)
				},
			)
			if err != nil {
				errCh <- err
			}
		}(prefix, tableName, shift)
	}
	wg.Wait()
	close(errCh)

	issues := make([]error, 0, v.GetInt(WorkersCount))
	for err := range errCh {
		issues = append(issues, err)
	}
	if len(issues) > 0 {
		log.Println("upserted rows with errors:", issues)

		// in go 1.20 better to replace with errors.Join()
		err := errors.New("")
		for _, issue := range issues {
			err = fmt.Errorf("%w, %w", err, issue)
		}

		return fmt.Errorf("could not upsert rows: %w", err)
	}

	return nil
}

func scanSelect(ctx context.Context, v *viper.Viper, c table.Client, prefix string) <-chan error {
	//span, ctx := opentracing.StartSpanFromContext(ctx, "scan select")
	//defer span.Finish()

	var (
		wg    sync.WaitGroup
		errCh = make(chan error, v.GetInt(WorkersCount))
	)
	wg.Add(v.GetInt(WorkersCount))

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for workerNum := 0; workerNum < v.GetInt(WorkersCount); workerNum++ {
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
				_, err := scanSelectJob(
					ctx,
					c,
					prefix,
					rand.Int63n(v.GetInt64(MaxLimit)),
				)
				if err != nil {
					errCh <- err
				}
			}
		}()
	}

	return errCh
}

func scanSelectJob(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "scan select job")
	defer func() {
		if err != nil {
			span.SetTag("error", true)
		} else {
			span.SetTag("error", false)
		}
		span.Finish()
	}()

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
