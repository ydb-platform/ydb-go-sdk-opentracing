package tracing

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes table.ClientTrace with solomon metrics publishing
func Table(opts ...option) trace.Table {
	h := &options{}
	for _, o := range opts {
		o(h)
	}
	if h.details == 0 {
		h.details = trace.DetailsAll
	}
	t := trace.Table{}
	if h.details&trace.TablePoolRetryEvents != 0 {
		t.OnPoolDo = func(info trace.PoolDoStartInfo) func(info trace.PoolDoInternalInfo) func(trace.PoolDoDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_do",
			)
			start.SetTag("idempotent", info.Idempotent)
			return func(info trace.PoolDoInternalInfo) func(trace.PoolDoDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.PoolDoDoneInfo) {
					finish(start,
						info.Error,
						"attempts", info.Attempts,
					)
				}
			}
		}
		t.OnPoolDoTx = func(info trace.PoolDoTxStartInfo) func(info trace.PoolDoTxInternalInfo) func(trace.PoolDoTxDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_table_do_tx",
			)
			start.SetTag("idempotent", info.Idempotent)
			return func(info trace.PoolDoTxInternalInfo) func(trace.PoolDoTxDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.PoolDoTxDoneInfo) {
					finish(start,
						info.Error,
						"attempts", info.Attempts,
					)
				}
			}
		}
	}
	if h.details&trace.TableSessionEvents != 0 {
		if h.details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_new",
				)
				return func(info trace.SessionNewDoneInfo) {
					if info.Session != nil {
						start.SetTag("nodeID", nodeID(info.Session.ID()))
						start.LogKV(
							"id", info.Session.ID(),
							"status", info.Session.Status(),
						)
					}
					finish(start, info.Error)
				}
			}
			t.OnSessionDelete = func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_delete",
					"id", info.Session.ID(),
				)
				start.SetTag("nodeID", nodeID(info.Session.ID()))
				return func(info trace.SessionDeleteDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_keep_alive",
					"id", info.Session.ID(),
				)
				start.SetTag("nodeID", nodeID(info.Session.ID()))
				return func(info trace.KeepAliveDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
		if h.details&trace.TableSessionQueryEvents != 0 {
			if h.details&trace.TableSessionQueryInvokeEvents != 0 {
				t.OnSessionQueryPrepare = func(info trace.SessionQueryPrepareStartInfo) func(trace.PrepareDataQueryDoneInfo) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_prepare",
						"id", info.Session.ID(),
						"query", info.Query,
					)
					start.SetTag("nodeID", nodeID(info.Session.ID()))
					return func(info trace.PrepareDataQueryDoneInfo) {
						finish(
							start,
							info.Error,
							"result", info.Result.String(),
						)
					}
				}
				t.OnSessionQueryExecute = func(info trace.ExecuteDataQueryStartInfo) func(trace.SessionQueryPrepareDoneInfo) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_execute",
						"id", info.Session.ID(),
						"query", info.Query,
						"params", info.Parameters.String(),
					)
					start.SetTag("nodeID", nodeID(info.Session.ID()))
					return func(info trace.SessionQueryPrepareDoneInfo) {
						finish(
							start,
							info.Error,
							"resultErr", info.Result.Err().Error(),
							"prepared", info.Prepared,
							"tx", info.Tx.ID(),
						)
					}
				}
			}
			if h.details&trace.TableSessionQueryStreamEvents != 0 {
				t.OnSessionQueryStreamExecute = func(info trace.SessionQueryStreamExecuteStartInfo) func(trace.SessionQueryStreamExecuteDoneInfo) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_stream_execute",
						"id", info.Session.ID(),
						"query", info.Query,
						"params", info.Parameters.String(),
					)
					start.SetTag("nodeID", nodeID(info.Session.ID()))
					return func(info trace.SessionQueryStreamExecuteDoneInfo) {
						finish(start, info.Error)
					}
				}
				t.OnSessionQueryStreamRead = func(info trace.SessionQueryStreamReadStartInfo) func(trace.SessionQueryStreamReadDoneInfo) {
					start := startSpan(
						info.Context,
						"ydb_table_session_query_stream_read",
						"id", info.Session.ID(),
					)
					start.SetTag("nodeID", nodeID(info.Session.ID()))
					return func(info trace.SessionQueryStreamReadDoneInfo) {
						finish(start, info.Error)
					}
				}
			}
		}
		if h.details&trace.TableSessionTransactionEvents != 0 {
			t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_begin",
					"id", info.Session.ID(),
				)
				start.SetTag("nodeID", nodeID(info.Session.ID()))
				return func(info trace.SessionTransactionBeginDoneInfo) {
					if info.Tx != nil {
						start.LogKV("tx", info.Tx.ID())
					}
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_commit",
					"id", info.Session.ID(),
					"tx", info.Tx.ID(),
				)
				start.SetTag("nodeID", nodeID(info.Session.ID()))
				return func(info trace.SessionTransactionCommitDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_session_tx_rollback",
					"id", info.Session.ID(),
					"tx", info.Tx.ID(),
				)
				start.SetTag("nodeID", nodeID(info.Session.ID()))
				return func(info trace.SessionTransactionRollbackDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
	}
	if h.details&trace.TablePoolEvents != 0 {
		if h.details&trace.TablePoolLifeCycleEvents != 0 {
			t.OnPoolInit = func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_init",
				)
				return func(info trace.PoolInitDoneInfo) {
					finish(
						start,
						nil,
						"min", info.KeepAliveMinSize,
						"max", info.Limit,
					)
				}
			}
			t.OnPoolClose = func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_close",
				)
				return func(info trace.PoolCloseDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
		if h.details&trace.TablePoolSessionLifeCycleEvents != 0 {
			t.OnPoolSessionNew = func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_session_new",
				)
				return func(info trace.PoolSessionNewDoneInfo) {
					if info.Session != nil {
						start.LogKV(
							"id", info.Session.ID(),
							"status", info.Session.Status(),
						)
						start.SetTag("nodeID", nodeID(info.Session.ID()))
					}
					finish(start, info.Error)
				}
			}
			t.OnPoolSessionClose = func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_session_close",
					"id", info.Session.ID(),
				)
				start.SetTag("nodeID", nodeID(info.Session.ID()))
				return func(info trace.PoolSessionCloseDoneInfo) {
					finish(start, nil)
				}
			}
		}
		if h.details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_put",
					"id", info.Session.ID(),
				)
				start.SetTag("nodeID", nodeID(info.Session.ID()))
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
					if info.Session != nil {
						start.LogKV(
							"id", info.Session.ID(),
							"status", info.Session.Status(),
						)
						start.SetTag("nodeID", nodeID(info.Session.ID()))
					}
					finish(
						start,
						info.Error,
						"attempts", info.Attempts,
					)
				}
			}
			t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_wait",
				)
				return func(info trace.PoolWaitDoneInfo) {
					if info.Session != nil {
						start.LogKV(
							"id", info.Session.ID(),
							"status", info.Session.Status(),
						)
						start.SetTag("nodeID", nodeID(info.Session.ID()))
					}
					finish(start, info.Error)
				}
			}
			t.OnPoolTake = func(info trace.PoolTakeStartInfo) func(doneInfo trace.PoolTakeWaitInfo) func(doneInfo trace.PoolTakeDoneInfo) {
				start := startSpan(
					info.Context,
					"ydb_table_pool_take",
					"id", info.Session.ID(),
				)
				start.SetTag("nodeID", nodeID(info.Session.ID()))
				return func(info trace.PoolTakeWaitInfo) func(info trace.PoolTakeDoneInfo) {
					return func(info trace.PoolTakeDoneInfo) {
						finish(start, nil)
					}
				}
			}
		}
	}
	return t
}
