package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"log"
	"os"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func sliceToInterfaces[T any](v []T) []interface{} {
	ii := make([]interface{}, len(v))
	for i, vv := range v {
		ii[i] = vv
	}
	return ii
}

func selectDefault(ctx context.Context, db *sql.DB) (err error) {
	// explain of query
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
		row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ExplainQueryMode), `
			SELECT series_id, title, release_date FROM series;
		`)
		var (
			ast  string
			plan string
		)
		if err = row.Scan(&ast, &plan); err != nil {
			return err
		}
		//log.Printf("AST = %s\n\nPlan = %s", ast, plan)
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("explain query failed: %w", err)
	}
	err = retry.Do(ydb.WithTxControl(ctx, table.OnlineReadOnlyTxControl()), db, func(ctx context.Context, cc *sql.Conn) (err error) {
		rows, err := cc.QueryContext(ctx, `
			SELECT series_id, title, release_date FROM series;
		`)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		var (
			id          *string
			title       *string
			releaseDate *time.Time
		)
		log.Println("> select of all known series:")
		for rows.Next() {
			if err = rows.Scan(&id, &title, &releaseDate); err != nil {
				return err
			}
			log.Printf(
				"> [%s] %s (%s)",
				*id, *title, releaseDate.Format("2006-01-02"),
			)
		}
		return rows.Err()
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("execute data query failed: %w", err)
	}
	return nil
}

func selectScan(ctx context.Context, db *sql.DB) (err error) {
	// scan query
	err = retry.Do(
		ydb.WithTxControl(ctx, table.StaleReadOnlyTxControl()), db,
		func(ctx context.Context, cc *sql.Conn) (err error) {
			var (
				id        string
				seriesIDs []types.Value
				seasonIDs []types.Value
			)
			// getting series ID's
			row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode), `
				SELECT 			series_id 		
				FROM 			series
				WHERE 			title LIKE $seriesTitle;
			`, sql.Named("seriesTitle", "%IT Crowd%"))
			if err = row.Scan(&id); err != nil {
				return err
			}
			seriesIDs = append(seriesIDs, types.BytesValueFromString(id))
			if err = row.Err(); err != nil {
				return err
			}

			// getting season ID's
			rows, err := cc.QueryContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode), `
				SELECT 			season_id 		
				FROM 			seasons
				WHERE 			title LIKE $seasonTitle
			`, sql.Named("seasonTitle", "%Season 1%"))
			if err != nil {
				return err
			}
			for rows.Next() {
				if err = rows.Scan(&id); err != nil {
					return err
				}
				seasonIDs = append(seasonIDs, types.BytesValueFromString(id))
			}
			if err = rows.Err(); err != nil {
				return err
			}
			_ = rows.Close()

			// getting final query result
			params := table.NewQueryParameters(
				table.ValueParam("seriesIDs", types.ListValue(seriesIDs...)),
				table.ValueParam("seasonIDs", types.ListValue(seasonIDs...)),
				table.ValueParam("from", types.DateValueFromTime(date("2006-01-01"))),
				table.ValueParam("to", types.DateValueFromTime(date("2006-12-31"))),
			)
			rows, err = cc.QueryContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode), `
				SELECT 
					episode_id, title, air_date FROM episodes
				WHERE 	
					series_id IN $seriesIDs 
					AND season_id IN $seasonIDs 
					AND air_date BETWEEN $from AND $to;`, params,
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = rows.Close()
			}()
			var (
				episodeID  string
				title      string
				firstAired time.Time
			)
			log.Println("> scan select of episodes of `Season 1` of `IT Crowd` between 2006-01-01 and 2006-12-31:")
			for rows.Next() {
				if err = rows.Scan(&episodeID, &title, &firstAired); err != nil {
					return err
				}
				log.Printf(
					"> [%s] %s (%s)",
					episodeID, title, firstAired.Format("2006-01-02"),
				)
			}
			return rows.Err()
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
	)
	if err != nil {
		return fmt.Errorf("scan query failed: %w", err)
	}
	return nil
}

func fillTablesWithData(ctx context.Context, db *sql.DB) (err error) {
	series, seasonsData, episodesData := getData()
	args := []sql.NamedArg{
		sql.Named("seriesData", types.ListValue(series...)),
		sql.Named("seasonsData", types.ListValue(seasonsData...)),
		sql.Named("episodesData", types.ListValue(episodesData...)),
	}
	err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		if _, err = tx.ExecContext(ctx, `
				REPLACE INTO series
				SELECT * FROM AS_TABLE($seriesData);
					
				REPLACE INTO seasons
				SELECT * FROM AS_TABLE($seasonsData);
					
				REPLACE INTO episodes
				SELECT * FROM AS_TABLE($episodesData);
			`, sliceToInterfaces(args)...,
		); err != nil {
			return err
		}
		return nil
	}, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("upsert query failed: %w", err)
	}
	return nil
}

func prepareSchema(ctx context.Context, db *sql.DB, prefix string) (err error) {
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		tableName := path.Join(prefix, "series")
		nativeDriver, err := ydb.Unwrap(db)
		if err != nil {
			return err
		}
		if exists, err := sugar.IsTableExists(ctx, nativeDriver.Scheme(), tableName); err != nil {
			return err
		} else if exists {
			_, err = cc.ExecContext(
				ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
				fmt.Sprintf("DROP TABLE `%s`", tableName),
			)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stdout, "warn: drop series table failed: %v", err)
			}
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), `
			CREATE TABLE `+"`"+path.Join(prefix, "series")+"`"+` (
				series_id Bytes,
				title Utf8,
				series_info Utf8,
				release_date Date,
				comment Utf8,
				INDEX index_series_title GLOBAL ASYNC ON ( title ),
				PRIMARY KEY (
					series_id
				)
			) WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			);`,
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create series table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		tableName := path.Join(prefix, "seasons")
		nativeDriver, err := ydb.Unwrap(db)
		if err != nil {
			return err
		}
		if exists, err := sugar.IsTableExists(ctx, nativeDriver.Scheme(), tableName); err != nil {
			return err
		} else if exists {
			_, err = cc.ExecContext(
				ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
				fmt.Sprintf("DROP TABLE `%s`", tableName),
			)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stdout, "warn: drop series table failed: %v", err)
			}
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), `
				CREATE TABLE `+"`"+path.Join(prefix, "seasons")+"`"+` (
					series_id Bytes,
					season_id Bytes,
					title Utf8,
					first_aired Date,
					last_aired Date,
					INDEX index_seasons_title GLOBAL ASYNC ON ( title ),
					INDEX index_seasons_first_aired GLOBAL ASYNC ON ( first_aired ),
					PRIMARY KEY (
						series_id,
						season_id
					)
				) WITH (
					AUTO_PARTITIONING_BY_LOAD = ENABLED
				);
			`,
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create seasons table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		tableName := path.Join(prefix, "episodes")
		nativeDriver, err := ydb.Unwrap(db)
		if err != nil {
			return err
		}
		if exists, err := sugar.IsTableExists(ctx, nativeDriver.Scheme(), tableName); err != nil {
			return err
		} else if exists {
			_, err = cc.ExecContext(
				ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
				fmt.Sprintf("DROP TABLE `%s`", tableName),
			)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stdout, "warn: drop series table failed: %v", err)
			}
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), `
			CREATE TABLE `+"`"+path.Join(prefix, "episodes")+"`"+` (
				series_id Bytes,
				season_id Bytes,
				episode_id Bytes,
				title Utf8,
				air_date Date,
				views Uint64,
				INDEX index_episodes_air_date GLOBAL ASYNC ON ( air_date ),
				PRIMARY KEY (
					series_id,
					season_id,
					episode_id
				)
			) WITH (
				AUTO_PARTITIONING_BY_LOAD = ENABLED
			);
		`)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create episodes table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	return nil
}
