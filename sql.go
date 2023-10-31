package tracing

import (
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/safe"
	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/str"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(details trace.Details) (t trace.DatabaseSQL) {
	if details&trace.DatabaseSQLEvents == 0 {
		return
	}
	prefix := "ydb_database_sql"
	if details&trace.DatabaseSQLConnectorEvents != 0 {
		//nolint:govet
		prefix := prefix + "_connector"
		t.OnConnectorConnect = func(
			info trace.DatabaseSQLConnectorConnectStartInfo,
		) func(
			trace.DatabaseSQLConnectorConnectDoneInfo,
		) {
			start := startSpan(
				info.Context,
				prefix+"_connect",
			)
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		prefix := prefix + "_conn"
		t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_ping",
			)
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_prepare",
				otlog.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_exec",
				otlog.String("query", info.Query),
				otlog.String("query_mode", info.Mode),
			)
			start.SetBaggageItem("idempotent", str.Bool(info.Idempotent))
			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_query",
				otlog.String("query", info.Query),
				otlog.String("query_mode", info.Mode),
			)
			start.SetBaggageItem("idempotent", str.Bool(info.Idempotent))
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		prefix := prefix + "_tx"
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_begin",
			)
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				if info.Error == nil {
					start.SetTag("transaction_id", safe.ID(info.Tx))
				}
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_rollback",
			)
			start.SetTag("transaction_id", safe.ID(info.Tx))
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_commit",
			)
			start.SetTag("transaction_id", safe.ID(info.Tx))
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnTxExec = func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
			start := followSpan(
				opentracing.SpanFromContext(info.TxContext).Context(),
				prefix+"_exec",
				otlog.String("query", info.Query),
			)
			start.SetTag("transaction_id", safe.ID(info.Tx))
			start.SetBaggageItem("idempotent", str.Bool(info.Idempotent))
			return func(info trace.DatabaseSQLTxExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnTxQuery = func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
			start := followSpan(
				opentracing.SpanFromContext(info.TxContext).Context(),
				prefix+"_query",
				otlog.String("query", info.Query),
			)
			start.SetTag("transaction_id", safe.ID(info.Tx))
			start.SetBaggageItem("idempotent", str.Bool(info.Idempotent))
			return func(info trace.DatabaseSQLTxQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	//nolint:nestif
	if details&trace.DatabaseSQLStmtEvents != 0 {
		//nolint:govet
		prefix := prefix + "_stmt"
		t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_exec",
				otlog.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_query",
				otlog.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	return t
}
