package tracing

import (
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/safe"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes table.ClientTrace with solomon metrics publishing
func Table(details trace.Details) (t trace.Table) {
	if details&trace.TablePoolRetryEvents != 0 {
		t.OnPoolDo = func(info trace.PoolDoStartInfo) func(info trace.PoolDoIntermediateInfo) func(trace.PoolDoDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_do",
			)
			start.SetTag("idempotent", info.Idempotent)
			return func(info trace.PoolDoIntermediateInfo) func(trace.PoolDoDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.PoolDoDoneInfo) {
					finish(start,
						info.Error,
						otlog.Int("attempts", info.Attempts),
					)
				}
			}
		}
		t.OnPoolDoTx = func(info trace.PoolDoTxStartInfo) func(info trace.PoolDoTxIntermediateInfo) func(trace.PoolDoTxDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_do_tx",
			)
			start.SetTag("idempotent", info.Idempotent)
			return func(info trace.PoolDoTxIntermediateInfo) func(trace.PoolDoTxDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.PoolDoTxDoneInfo) {
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
			t.OnSessionNew = func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_new",
				)
				return func(info trace.SessionNewDoneInfo) {
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					finish(
						start,
						info.Error,
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("status", safe.Status(info.Session)),
					)
				}
			}
			t.OnSessionDelete = func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_delete",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.SessionDeleteDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_keep_alive",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.KeepAliveDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
		if details&trace.TableSessionQueryEvents != 0 {
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				t.OnSessionQueryPrepare = func(
					info trace.PrepareDataQueryStartInfo,
				) func(
					trace.PrepareDataQueryDoneInfo,
				) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_prepare",
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("query", info.Query),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					return func(info trace.PrepareDataQueryDoneInfo) {
						finish(
							start,
							info.Error,
							otlog.String("result", safe.Stringer(info.Result)),
						)
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.ExecuteDataQueryStartInfo,
				) func(
					trace.ExecuteDataQueryDoneInfo,
				) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_execute",
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("query", safe.Stringer(info.Query)),
						otlog.String("params", safe.Stringer(info.Parameters)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					return func(info trace.ExecuteDataQueryDoneInfo) {
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
					info trace.SessionQueryStreamExecuteStartInfo,
				) func(
					intermediateInfo trace.SessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.SessionQueryStreamExecuteDoneInfo,
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
						info trace.SessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.SessionQueryStreamExecuteDoneInfo,
					) {
						intermediate(start, info.Error)
						return func(info trace.SessionQueryStreamExecuteDoneInfo) {
							finish(start, info.Error)
						}
					}
				}
				t.OnSessionQueryStreamRead = func(
					info trace.SessionQueryStreamReadStartInfo,
				) func(
					trace.SessionQueryStreamReadIntermediateInfo,
				) func(
					trace.SessionQueryStreamReadDoneInfo,
				) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_stream_read",
						otlog.String("id", safe.ID(info.Session)),
					)
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					return func(
						info trace.SessionQueryStreamReadIntermediateInfo,
					) func(
						trace.SessionQueryStreamReadDoneInfo,
					) {
						intermediate(start, info.Error)
						return func(info trace.SessionQueryStreamReadDoneInfo) {
							finish(start, info.Error)
						}
					}
				}
			}
		}
		if details&trace.TableSessionTransactionEvents != 0 {
			t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_begin",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.SessionTransactionBeginDoneInfo) {
					finish(
						start,
						info.Error,
						otlog.String("tx", safe.ID(info.Tx)),
					)
				}
			}
			t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_commit",
					otlog.String("id", safe.ID(info.Session)),
					otlog.String("tx", safe.ID(info.Tx)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.SessionTransactionCommitDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_rollback",
					otlog.String("id", safe.ID(info.Session)),
					otlog.String("tx", safe.ID(info.Tx)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.SessionTransactionRollbackDoneInfo) {
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
			t.OnPoolSessionNew = func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_session_new",
				)
				return func(info trace.PoolSessionNewDoneInfo) {
					start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
					finish(
						start,
						info.Error,
						otlog.String("id", safe.ID(info.Session)),
						otlog.String("status", safe.Status(info.Session)),
					)
				}
			}
			t.OnPoolSessionClose = func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_session_close",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.PoolSessionCloseDoneInfo) {
					finish(start, nil)
				}
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_put",
					otlog.String("id", safe.ID(info.Session)),
				)
				start.SetTag("nodeID", nodeID(safe.ID(info.Session)))
				return func(info trace.PoolPutDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnPoolGet = func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_get",
				)
				return func(info trace.PoolGetDoneInfo) {
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
			t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_wait",
				)
				return func(info trace.PoolWaitDoneInfo) {
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
