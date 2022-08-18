package main

import (
	"context"
	"database/sql"
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

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	jaegerConfig "github.com/uber/jaeger-client-go/config"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	tracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
}

const (
	tracerURL   = "localhost:5775"
	serviceName = "bench"
	prefix      = "ydb-go-sdk-opentracing/bench/database-sql"
)

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

	creds := ydb.WithAnonymousCredentials()
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		creds = ydb.WithAccessTokenCredentials(token)
	}
	cc, err := ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithBalancer(balancers.RandomChoice()),
		creds,
		ydb.WithSessionPoolSizeLimit(300),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		tracing.WithTraces(trace.DatabaseSQLEvents|trace.TableEvents),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = cc.Close(ctx)
	}()

	connector, err := ydb.Connector(cc)
	if err != nil {
		panic(err)
	}

	db := sql.OpenDB(connector)
	defer func() {
		_ = db.Close()
	}()

	db.SetMaxOpenConns(1000)
	db.SetMaxIdleConns(1000)

	if v, err := strconv.Atoi(os.Getenv("YDB_PREPARE_SCHEME")); err != nil || v != 0 {
		if err = prepareSchema(ctx, db, path.Join(cc.Name(), prefix)); err != nil {
			log.Fatal(err)
		}
	}

	if concurrency, err := strconv.Atoi(os.Getenv("YDB_PREPARE_DATA_CONCURRENCY")); err != nil || concurrency != 0 {
		if concurrency == 0 {
			concurrency = 100
		}
		if err = upsertData(ctx, db, path.Join(cc.Name(), prefix), concurrency); err != nil {
			log.Fatal(err)
		}
	}

	if concurrency, err := strconv.Atoi(os.Getenv("YDB_READ_DATA_CONCURRENCY")); err != nil || concurrency != 0 {
		if concurrency == 0 {
			concurrency = 100
		}

		wg := sync.WaitGroup{}
		sema := make(chan struct{}, concurrency)
		for {
			select {
			case sema <- struct{}{}:
				wg.Add(1)
				go func() {
					defer func() {
						wg.Done()
						<-sema
					}()
					if rand.Int63n(2) != 0 {
						ctx = ydb.WithQueryMode(ctx, ydb.ScanQueryMode)
					}
					_, _ = readData(ctx, db, path.Join(cc.Name(), prefix), rand.Int63n(25000))
				}()
			case <-ctx.Done():
				break
			}

		}
		wg.Wait()
	}
}

func prepareSchema(ctx context.Context, db *sql.DB, prefix string) (err error) {
	if err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err := cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "series")),
		)
		if err != nil {
			fmt.Fprintf(os.Stdout, "warn: drop series table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`CREATE TABLE `+"`"+path.Join(prefix, "series")+"`"+` (
				series_id UTF8,
				title UTF8,
				series_info UTF8,
				release_date Date,
				comment UTF8,
				PRIMARY KEY (
					series_id
				)
			) WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			);`,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create series table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true))); err != nil {
		return err
	}
	if err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "seasons")),
		)
		if err != nil {
			fmt.Fprintf(os.Stdout, "warn: drop seasons table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`CREATE TABLE `+"`"+path.Join(prefix, "seasons")+"`"+` (
				series_id UTF8,
				season_id UTF8,
				title UTF8,
				first_aired Date,
				last_aired Date,
				PRIMARY KEY (
					series_id,
					season_id
				)
			) WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			);`,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create seasons table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true))); err != nil {
		return err
	}
	if err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "episodes")),
		)
		if err != nil {
			fmt.Fprintf(os.Stdout, "warn: drop episodes table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`CREATE TABLE `+"`"+path.Join(prefix, "episodes")+"`"+` (
				series_id UTF8,
				season_id UTF8,
				episode_id UTF8,
				title UTF8,
				air_date Date,
				views Uint64,
				PRIMARY KEY (
					series_id,
					season_id,
					episode_id
				)
			) WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			);`,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create episodes table failed: %v", err)
			return err
		}

		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true))); err != nil {
		return err
	}
	return nil
}

func upsertData(ctx context.Context, db *sql.DB, prefix string, concurrency int) (err error) {
	rowsLen := 25000000000
	batchSize := 1000
	wg := sync.WaitGroup{}
	sema := make(chan struct{}, concurrency)
	for shift := 0; shift < rowsLen; shift += batchSize {
		wg.Add(1)
		sema <- struct{}{}
		go func(prefix string, shift int) {
			defer func() {
				<-sema
				wg.Done()
			}()
			rows := make([]types.Value, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("series_id", types.UTF8Value(uuid.New().String())),
					types.StructFieldValue("title", types.UTF8Value(fmt.Sprintf("series No. %d title", i+shift+3))),
					types.StructFieldValue("series_info", types.UTF8Value(fmt.Sprintf("series No. %d info", i+shift+3))),
					types.StructFieldValue("release_date", types.DateValueFromTime(time.Now())),
					types.StructFieldValue("comment", types.UTF8Value(fmt.Sprintf("series No. %d comment", i+shift+3))),
				))
			}
			if err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx,
					fmt.Sprintf("UPSERT INTO `%s` SELECT * FROM AS_TABLE($values)", path.Join(prefix, "series")),
					sql.Named("values", types.ListValue(rows...)),
				); err != nil {
					return err
				}
				return nil
			}, retry.WithDoTxRetryOptions(retry.WithIdempotent(true))); err != nil {
				fmt.Fprintf(os.Stderr, "Upsert failed: %v", err)
			}
		}(prefix, shift)
	}
	wg.Wait()
	return nil
}

func readData(ctx context.Context, db *sql.DB, prefix string, limit int64) (count uint64, err error) {
	var query = fmt.Sprintf("SELECT series_id, title, release_date FROM `%s` LIMIT %d;",
		path.Join(prefix, "series"),
		limit,
	)
	if err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		rows, err := cc.QueryContext(ctx, query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "scan failed: %v", err)
			return err
		}
		defer rows.Close()
		var (
			id    string
			title *string
			date  *time.Time
		)
		for rows.Next() {
			count++
			if err = rows.Scan(&id, &title, &date); err != nil {
				fmt.Fprintf(os.Stderr, "scan failed: %v", err)
				return err
			}
		}
		if err = rows.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "rows err not nil: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true))); err != nil {
		fmt.Fprintf(os.Stderr, "scan failed: %v", err)
		return count, err
	}
	return count, nil
}
