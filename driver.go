package tracing

import (
	otlog "github.com/opentracing/opentracing-go/log"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-opentracing/internal/safe"
)

// Driver makes Driver with publishing traces
func Driver(details trace.Details) (t trace.Driver) {
	if details&trace.DriverNetEvents != 0 {
		t.OnNetDial = func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_net_dial",
			)
			start.SetTag("address", info.Address)
			return func(info trace.DriverNetDialDoneInfo) {
				finish(start, info.Error)
			}
		}
	}
	if details&trace.DriverRepeaterEvents != 0 {
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterTickStartInfo) func(trace.DriverRepeaterTickDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_repeater_wake_up",
			)
			start.SetTag("name", info.Name)
			start.SetTag("event", info.Event)
			return func(info trace.DriverRepeaterTickDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	if details&trace.DriverConnEvents != 0 {
		t.OnConnTake = func(info trace.DriverConnTakeStartInfo) func(trace.DriverConnTakeDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_take",
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			return func(info trace.DriverConnTakeDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_invoke",
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			start.SetTag("method", string(info.Method))
			return func(info trace.DriverConnInvokeDoneInfo) {
				finish(
					start,
					info.Error,
					otlog.Object("issues", info.Issues),
					otlog.String("opID", info.OpID),
					otlog.String("state", safe.Stringer(info.State)),
				)
			}
		}
		t.OnConnNewStream = func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_new_stream",
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			start.SetTag("method", string(info.Method))
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.DriverConnNewStreamDoneInfo) {
					finish(
						start,
						info.Error,
						otlog.String("state", safe.Stringer(info.State)),
					)
				}
			}
		}
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_park",
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			return func(info trace.DriverConnParkDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_close",
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			return func(info trace.DriverConnCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	if details&trace.DriverClusterEvents != 0 {
		t.OnClusterInit = func(info trace.DriverClusterInitStartInfo) func(trace.DriverClusterInitDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_init",
			)
			return func(info trace.DriverClusterInitDoneInfo) {
				finish(start, nil)
			}
		}
		t.OnClusterClose = func(info trace.DriverClusterCloseStartInfo) func(trace.DriverClusterCloseDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_close",
			)
			return func(info trace.DriverClusterCloseDoneInfo) {
				finish(start, info.Error)
			}
		}
		t.OnClusterGet = func(info trace.DriverClusterGetStartInfo) func(trace.DriverClusterGetDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_get",
			)
			return func(info trace.DriverClusterGetDoneInfo) {
				if info.Error == nil {
					start.SetTag("address", safe.Address(info.Endpoint))
					start.SetTag("nodeID", safe.NodeID(info.Endpoint))
				}
				finish(start, info.Error)
			}
		}
		t.OnClusterInsert = func(info trace.DriverClusterInsertStartInfo) func(trace.DriverClusterInsertDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_insert",
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			start.SetTag("nodeID", safe.NodeID(info.Endpoint))
			return func(info trace.DriverClusterInsertDoneInfo) {
				finish(
					start,
					nil,
					otlog.String("state", safe.Stringer(info.State)),
				)
			}
		}
		t.OnClusterRemove = func(info trace.DriverClusterRemoveStartInfo) func(trace.DriverClusterRemoveDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_remove",
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			start.SetTag("nodeID", safe.NodeID(info.Endpoint))
			return func(info trace.DriverClusterRemoveDoneInfo) {
				finish(
					start,
					nil,
					otlog.String("state", safe.Stringer(info.State)),
				)
			}
		}
		t.OnClusterUpdate = func(info trace.DriverClusterUpdateStartInfo) func(trace.DriverClusterUpdateDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_update",
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			start.SetTag("nodeID", safe.NodeID(info.Endpoint))
			return func(info trace.DriverClusterUpdateDoneInfo) {
				finish(
					start,
					nil,
					otlog.String("state", safe.Stringer(info.State)),
				)
			}
		}
		t.OnPessimizeNode = func(info trace.DriverPessimizeNodeStartInfo) func(trace.DriverPessimizeNodeDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_pessimize",
				otlog.String("state", safe.Stringer(info.State)),
				otlog.String("cause", safe.Error(info.Cause)),
			)
			start.SetTag("address", safe.Address(info.Endpoint))
			start.SetTag("nodeID", safe.NodeID(info.Endpoint))
			return func(info trace.DriverPessimizeNodeDoneInfo) {
				finish(
					start,
					nil,
					otlog.String("state", safe.Stringer(info.State)),
				)
			}
		}
	}
	if details&trace.DriverCredentialsEvents != 0 {
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_credentials_get",
			)
			return func(info trace.DriverGetCredentialsDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	connectionsTotal := startSpanWithCounter(nil, "ydb_connections", "total")
	return t.Compose(trace.Driver{
		OnInit: func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_driver_init",
			)
			start.SetTag("endpoint", info.Endpoint)
			start.SetTag("database", info.Database)
			start.SetTag("secure", info.Secure)
			return func(info trace.DriverInitDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnClose: func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			connectionsTotal.span.Finish()
			start := startSpan(
				info.Context,
				"ydb_driver_close",
			)
			return func(info trace.DriverCloseDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnNetDial: func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			return func(info trace.DriverNetDialDoneInfo) {
				if info.Error == nil {
					connectionsTotal.add(1)
				}
			}
		},
		OnNetClose: func(info trace.DriverNetCloseStartInfo) func(trace.DriverNetCloseDoneInfo) {
			connectionsTotal.add(-1)
			return nil
		},
	})
}
