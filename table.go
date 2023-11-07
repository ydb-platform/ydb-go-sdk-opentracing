package tracing

import (
	"fmt"

	"github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/safe"
	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/str"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes table.ClientTrace with solomon metrics publishing
func Table(details trace.Details) (t trace.Table) {
	if details&trace.TableEvents != 0 {
		t.OnCreateSession = func(info trace.TableCreateSessionStartInfo) func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_create_session",
			)
			return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
				s := followSpan(start.Context(), "ydb_table_create_session_intermediate")
				if info.Error != nil {
					logError(s, info.Error)
				}
				s.Finish()
				return func(info trace.TableCreateSessionDoneInfo) {
					s := followSpan(start.Context(), "ydb_table_create_session_done",
						otlog.Int("attempts", info.Attempts),
						otlog.String("session_id", safe.ID(info.Session)),
					)
					if info.Error != nil {
						logError(s, info.Error)
					}
					s.Finish()
				}
			}
		}
		t.OnDo = func(info trace.TableDoStartInfo) func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_do",
			)
			if info.NestedCall {
				start.SetTag(string(ext.Error), true)
				start.LogFields(otlog.Error(fmt.Errorf("nested call")))
			}
			start.SetBaggageItem("idempotent", str.Bool(info.Idempotent))
			start.Finish()
			return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				s := followSpan(start.Context(), "ydb_table_do_intermediate")
				if info.Error != nil {
					logError(s, info.Error)
				}
				s.Finish()
				return func(info trace.TableDoDoneInfo) {
					s := followSpan(start.Context(), "ydb_table_do_done",
						otlog.Int("attempts", info.Attempts),
					)
					if info.Error != nil {
						logError(s, info.Error)
					}
					s.Finish()
				}
			}
		}
		t.OnDoTx = func(info trace.TableDoTxStartInfo) func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_do_tx",
			)
			if info.NestedCall {
				start.SetTag(string(ext.Error), true)
				start.LogFields(otlog.Error(fmt.Errorf("nested call")))
			}
			start.SetBaggageItem("idempotent", str.Bool(info.Idempotent))
			start.Finish()
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				s := followSpan(start.Context(), "ydb_table_do_tx_intermediate")
				if info.Error != nil {
					logError(s, info.Error)
				}
				s.Finish()
				return func(info trace.TableDoTxDoneInfo) {
					s := followSpan(start.Context(), "ydb_table_do_tx_done",
						otlog.Int("attempts", info.Attempts),
					)
					if info.Error != nil {
						logError(s, info.Error)
					}
					s.Finish()
				}
			}
		}
	}
	if details&trace.TableSessionEvents != 0 {
		if details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_new",
				)
				return func(info trace.TableSessionNewDoneInfo) {
					finish(
						start,
						info.Error,
						otlog.String("status", safe.Status(info.Session)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					start.SetTag("session_id", safe.ID(info.Session))
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_delete",
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				start.SetTag("session_id", safe.ID(info.Session))
				return func(info trace.TableSessionDeleteDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_keep_alive",
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				start.SetTag("session_id", safe.ID(info.Session))
				return func(info trace.TableKeepAliveDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
		if details&trace.TableSessionQueryEvents != 0 {
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				t.OnSessionQueryPrepare = func(
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_prepare",
						otlog.String("query", info.Query),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					start.SetTag("session_id", safe.ID(info.Session))
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						finish(
							start,
							info.Error,
							otlog.String("result", safe.Stringer(info.Result)),
						)
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.TableExecuteDataQueryStartInfo,
				) func(
					trace.TableExecuteDataQueryDoneInfo,
				) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_execute",
						otlog.String("query", safe.Stringer(info.Query)),
						otlog.Bool("keep_in_cache", info.KeepInCache),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					start.SetTag("session_id", safe.ID(info.Session))
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							start.SetTag("transaction_id", safe.ID(info.Tx))
							finish(
								start,
								safe.Err(info.Result),
								otlog.Bool("prepared", info.Prepared),
							)
						} else {
							finish(
								start,
								info.Error,
							)
						}
					}
				}
			}
			if details&trace.TableSessionQueryStreamEvents != 0 {
				t.OnSessionQueryStreamExecute = func(
					info trace.TableSessionQueryStreamExecuteStartInfo,
				) func(
					intermediateInfo trace.TableSessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamExecuteDoneInfo,
				) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_stream_execute",
						otlog.String("query", safe.Stringer(info.Query)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					start.SetTag("session_id", safe.ID(info.Session))
					start.Finish()
					return func(
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						s := followSpan(start.Context(), "ydb_table_session_query_stream_execute_intermediate")
						if info.Error != nil {
							logError(s, info.Error)
						}
						s.Finish()
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							s := followSpan(start.Context(), "ydb_table_session_query_stream_execute_done")
							if info.Error != nil {
								logError(s, info.Error)
							}
							s.Finish()
						}
					}
				}
				t.OnSessionQueryStreamRead = func(
					info trace.TableSessionQueryStreamReadStartInfo,
				) func(
					trace.TableSessionQueryStreamReadIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamReadDoneInfo,
				) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_stream_read",
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					start.SetTag("session_id", safe.ID(info.Session))
					start.Finish()
					return func(
						info trace.TableSessionQueryStreamReadIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamReadDoneInfo,
					) {
						s := followSpan(start.Context(), "ydb_table_session_query_stream_read_intermediate")
						if info.Error != nil {
							logError(s, info.Error)
						}
						s.Finish()
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							s := followSpan(start.Context(), "ydb_table_session_query_stream_read_done")
							if info.Error != nil {
								logError(s, info.Error)
							}
							s.Finish()
						}
					}
				}
			}
		}
		if details&trace.TableSessionTransactionEvents != 0 {
			t.OnSessionTransactionBegin = func(info trace.TableSessionTransactionBeginStartInfo) func(trace.TableSessionTransactionBeginDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_begin",
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				start.SetTag("session_id", safe.ID(info.Session))
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					start.SetTag("transaction_id", safe.ID(info.Tx))
					finish(
						start,
						info.Error,
					)
				}
			}
			t.OnSessionTransactionCommit = func(info trace.TableSessionTransactionCommitStartInfo) func(trace.TableSessionTransactionCommitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_commit",
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				start.SetTag("session_id", safe.ID(info.Session))
				start.SetTag("transaction_id", safe.ID(info.Tx))
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionRollback = func(info trace.TableSessionTransactionRollbackStartInfo) func(trace.TableSessionTransactionRollbackDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_rollback",
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				start.SetTag("session_id", safe.ID(info.Session))
				start.SetTag("transaction_id", safe.ID(info.Tx))
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionExecute = func(info trace.TableTransactionExecuteStartInfo) func(trace.TableTransactionExecuteDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_execute",
					otlog.String("query", safe.Stringer(info.Query)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				start.SetTag("session_id", safe.ID(info.Session))
				start.SetTag("transaction_id", safe.ID(info.Tx))
				return func(info trace.TableTransactionExecuteDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionExecuteStatement = func(info trace.TableTransactionExecuteStatementStartInfo) func(info trace.TableTransactionExecuteStatementDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_execute_statement",
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				start.SetTag("session_id", safe.ID(info.Session))
				start.SetTag("query", safe.Stringer(info.StatementQuery))
				start.SetTag("transaction_id", safe.ID(info.Tx))
				return func(info trace.TableTransactionExecuteStatementDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
	}
	if details&trace.TablePoolEvents != 0 {
		if details&trace.TablePoolLifeCycleEvents != 0 {
			t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_init",
				)
				return func(info trace.TableInitDoneInfo) {
					finish(
						start,
						nil,
						otlog.Int("limit", info.Limit),
					)
				}
			}
			t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_close",
				)
				return func(info trace.TableCloseDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_put",
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				start.SetTag("session_id", safe.ID(info.Session))
				return func(info trace.TablePoolPutDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_get",
				)
				return func(info trace.TablePoolGetDoneInfo) {
					finish(
						start,
						info.Error,
						otlog.Int("attempts", info.Attempts),
						otlog.String("status", safe.Status(info.Session)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					start.SetTag("session_id", safe.ID(info.Session))
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_wait",
				)
				return func(info trace.TablePoolWaitDoneInfo) {
					finish(
						start,
						info.Error,
						otlog.String("status", safe.Status(info.Session)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					start.SetTag("session_id", safe.ID(info.Session))
				}
			}
		}
	}
	return t
}
