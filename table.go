package tracing

import (
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/safe"
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
				intermediate(start, info.Error)
				return func(info trace.TableCreateSessionDoneInfo) {
					finish(start,
						info.Error,
						otlog.Int("attempts", info.Attempts),
					)
				}
			}
		}
		t.OnDo = func(info trace.TableDoStartInfo) func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_do",
			)
			start.SetTag("idempotent", info.Idempotent)
			return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.TableDoDoneInfo) {
					finish(start,
						info.Error,
						otlog.Int("attempts", info.Attempts),
					)
				}
			}
		}
		t.OnDoTx = func(info trace.TableDoTxStartInfo) func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_do_tx",
			)
			start.SetTag("idempotent", info.Idempotent)
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.TableDoTxDoneInfo) {
					finish(start,
						info.Error,
						otlog.Int("attempts", info.Attempts),
					)
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
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					finish(
						start,
						info.Error,
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("status", safe.Status(info.Session)),
					)
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_delete",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.TableSessionDeleteDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_keep_alive",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
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
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("query", info.Query),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
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
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("query", safe.Stringer(info.Query)),
						otlog.String("params", safe.Stringer(info.Parameters)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							finish(
								start,
								safe.Err(info.Result),
								otlog.Bool("prepared", info.Prepared),
								otlog.String("tx", safe.ID(info.Tx)),
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
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("query", safe.Stringer(info.Query)),
						otlog.String("params", safe.Stringer(info.Parameters)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					return func(
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						intermediate(start, info.Error)
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							finish(start, info.Error)
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
						otlog.String("id", safe.ID(info.Session)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					return func(
						info trace.TableSessionQueryStreamReadIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamReadDoneInfo,
					) {
						intermediate(start, info.Error)
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							finish(start, info.Error)
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
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					finish(
						start,
						info.Error,
						otlog.String("tx", safe.ID(info.Tx)),
					)
				}
			}
			t.OnSessionTransactionCommit = func(info trace.TableSessionTransactionCommitStartInfo) func(trace.TableSessionTransactionCommitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_commit",
					otlog.String("id", safe.ID(info.Session)),
					otlog.String("tx", safe.ID(info.Tx)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionRollback = func(info trace.TableSessionTransactionRollbackStartInfo) func(trace.TableSessionTransactionRollbackDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_rollback",
					otlog.String("id", safe.ID(info.Session)),
					otlog.String("tx", safe.ID(info.Tx)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
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
						otlog.Int("min", info.KeepAliveMinSize),
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
		if details&trace.TablePoolSessionLifeCycleEvents != 0 {
			t.OnPoolSessionNew = func(info trace.TablePoolSessionNewStartInfo) func(trace.TablePoolSessionNewDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_session_new",
				)
				return func(info trace.TablePoolSessionNewDoneInfo) {
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					finish(
						start,
						info.Error,
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("status", safe.Status(info.Session)),
					)
				}
			}
			t.OnPoolSessionClose = func(info trace.TablePoolSessionCloseStartInfo) func(trace.TablePoolSessionCloseDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_session_close",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.TablePoolSessionCloseDoneInfo) {
					finish(start, nil)
				}
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_put",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
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
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					finish(
						start,
						info.Error,
						otlog.Int("attempts", info.Attempts),
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("status", safe.Status(info.Session)),
					)
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_wait",
				)
				return func(info trace.TablePoolWaitDoneInfo) {
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					finish(
						start,
						info.Error,
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("status", safe.Status(info.Session)),
					)
				}
			}
		}
	}
	return t
}
