package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	jaegerConfig "github.com/uber/jaeger-client-go/config"

	env "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	tracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
}

const (
	serviceName = "ydb-go-sdk"
)

func testQuery(ctx context.Context, db *ydb.Driver) error {
	const query = `SELECT 42 as id, "myStr" as myStr;`

	// Do retry operation on errors with best effort
	err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, _, err = s.Execute(ctx, table.DefaultTxControl(), query, nil)
		if err != nil {
			return err
		}
		return err
	})
	return err
}

func main() {
	log.Println("started")

	tracer, closer, err := jaegerConfig.Configuration{
		ServiceName: serviceName,
		Sampler: &jaegerConfig.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &jaegerConfig.ReporterConfig{
			LogSpans:            true,
			BufferFlushInterval: 1 * time.Second,
			CollectorEndpoint:   os.Getenv("JAEGER_ENDPOINT"),
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

	db, err := ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithBalancer(balancers.RandomChoice()),
		env.WithEnvironCredentials(ctx),
		ydb.WithSessionPoolSizeLimit(300),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		tracing.WithTraces(trace.DetailsAll),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	log.Println("connected")

	if err := testQuery(ctx, db); err != nil {
		panic(err)
	}
	log.Println("test query done")

	workersCount, err := strconv.Atoi(os.Getenv("WORKERS_COUNT"))
	if err != nil {
		log.Fatal(err)
	}

	//TODO: remade to bool
	if os.Getenv("PREPARE_BENCH_DATA") == "1" {
		var tableName = "series"

		err = prepareTable(ctx, db.Table(), db.Name(), tableName)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("table prepared")

		err = upsertData(ctx, db.Table(), db.Name(), tableName, workersCount)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("upserted data")
	}

	maxLimit, _ := strconv.Atoi(os.Getenv("MAX_LIMIT"))
	errCh := scanSelect(ctx, db.Table(), db.Name(), rand.Int63n(int64(maxLimit)), workersCount)
	for err := range errCh {
		log.Println(err)
	}
}

func prepareTable(ctx context.Context, c table.Client, prefix, tableName string) (err error) {
	log.Println("dropping table", path.Join(prefix, tableName))
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
		return fmt.Errorf("error with creating table: %w", err)
	}

	log.Println("created table", path.Join(prefix, tableName))
	return nil
}

// TODO: remade to worker pool
func upsertData(ctx context.Context, c table.Client, prefix, tableName string, workersCount int) (err error) {

	log.Println("upserting rows")
	rowsLen, _ := strconv.Atoi(os.Getenv("ROWS_LEN"))
	batchSize, _ := strconv.Atoi(os.Getenv("BATCH_SIZE"))

	var (
		wg    = sync.WaitGroup{}
		sema  = make(chan struct{}, workersCount)
		errCh = make(chan error, workersCount)
	)

	for shift := 0; shift < rowsLen; shift += batchSize {
		wg.Add(1)
		sema <- struct{}{}
		go func(prefix, tableName string, shift int) {
			log.Println("upserting with shift", shift)
			defer func() {
				log.Println("finished upserting with shift", shift)
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

	issues := make([]error, 0, workersCount)
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

		return errors.New("could not upsert rows")
	}

	return nil
}

func scanSelect(ctx context.Context, c table.Client, prefix string, limit int64, workersCount int) <-chan error {
	var (
		wg    sync.WaitGroup
		errCh = make(chan error, workersCount)
	)
	wg.Add(workersCount)

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for workerNum := 0; workerNum < workersCount; workerNum++ {
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
				_, err := scanSelectJob(
					ctx,
					c,
					prefix,
					limit,
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
