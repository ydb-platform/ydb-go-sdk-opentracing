package tracing

import (
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
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			start := startSpan(
				info.Context,
				prefix+"_begin",
			)
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
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
			)
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
			)
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
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
