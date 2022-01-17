package tracing

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes Driver with publishing traces
func Driver(opts ...option) trace.Driver {
	h := &options{}
	for _, o := range opts {
		o(h)
	}
	if h.details == 0 {
		h.details = trace.DetailsAll
	}
	t := trace.Driver{}
	if h.details&trace.DriverNetEvents != 0 {
		t.OnNetDial = func(info trace.NetDialStartInfo) func(trace.NetDialDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_net_dial",
			)
			start.SetTag("address", info.Address)
			return func(info trace.NetDialDoneInfo) {
				finish(start, info.Error)
			}
		}
	}
	if h.details&trace.DriverCoreEvents != 0 {
		t.OnConnTake = func(info trace.ConnTakeStartInfo) func(trace.ConnTakeDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_take",
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			return func(info trace.ConnTakeDoneInfo) {
				finish(
					start,
					info.Error,
					"lock", info.Lock,
				)
			}
		}
		t.OnConnRelease = func(info trace.ConnReleaseStartInfo) func(trace.ConnReleaseDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_release",
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			return func(info trace.ConnReleaseDoneInfo) {
				finish(
					start,
					nil,
					"lock", info.Lock,
				)
			}
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_state_change",
				"state", info.State.String(),
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			return func(info trace.ConnStateChangeDoneInfo) {
				finish(
					start,
					nil,
					"state", info.State.String(),
				)
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_invoke",
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			start.SetTag("method", string(info.Method))
			return func(info trace.ConnInvokeDoneInfo) {
				finish(
					start,
					info.Error,
					"opID", info.OpID,
					"state", info.State.String(),
				)
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_new_stream",
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			start.SetTag("method", string(info.Method))
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.ConnNewStreamDoneInfo) {
					finish(
						start,
						info.Error,
						"state", info.State.String(),
					)
				}
			}
		}
	}
	if h.details&trace.DriverDiscoveryEvents != 0 {
		t.OnDiscovery = func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_discovery",
			)
			start.SetTag("address", info.Address)
			return func(info trace.DiscoveryDoneInfo) {
				finish(
					start,
					info.Error,
					"endpoints", info.Endpoints,
				)
			}
		}
	}
	if h.details&trace.DriverClusterEvents != 0 {
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_get",
			)
			return func(info trace.ClusterGetDoneInfo) {
				start.SetTag("address", info.Endpoint.Address())
				start.SetTag("local", info.Endpoint.LocalDC())
				start.SetTag("nodeID", info.Endpoint.NodeID())
				finish(start, info.Error)
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_insert",
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			start.SetTag("nodeID", info.Endpoint.NodeID())
			return func(info trace.ClusterInsertDoneInfo) {
				finish(
					start,
					nil,
					"state", info.State.String(),
				)
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_remove",
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			start.SetTag("nodeID", info.Endpoint.NodeID())
			return func(info trace.ClusterRemoveDoneInfo) {
				finish(
					start,
					nil,
					"state", info.State.String(),
				)
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_update",
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			start.SetTag("nodeID", info.Endpoint.NodeID())
			return func(info trace.ClusterUpdateDoneInfo) {
				finish(
					start,
					nil,
					"state", info.State.String(),
				)
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_cluster_pessimize",
				"state", info.State.String(),
				"cause", info.Cause.Error(),
			)
			start.SetTag("address", info.Endpoint.Address())
			start.SetTag("local", info.Endpoint.LocalDC())
			start.SetTag("nodeID", info.Endpoint.NodeID())
			return func(info trace.PessimizeNodeDoneInfo) {
				finish(
					start,
					nil,
					"state", info.State.String(),
				)
			}
		}
	}
	if h.details&trace.DriverCredentialsEvents != 0 {
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_credentials_get",
			)
			return func(info trace.GetCredentialsDoneInfo) {
				finish(
					start,
					info.Error,
					"tokenOk", info.TokenOk,
				)
			}
		}
	}
	connectionsTotal := startSpanWithCounter(nil, "ydb_connections", "total")
	return t.Compose(trace.Driver{
		OnInit: func(info trace.InitStartInfo) func(trace.InitDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_driver_init",
			)
			return func(info trace.InitDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnClose: func(info trace.CloseStartInfo) func(trace.CloseDoneInfo) {
			connectionsTotal.span.Finish()
			start := startSpan(
				info.Context,
				"ydb_driver_close",
			)
			return func(info trace.CloseDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnNetDial: func(info trace.NetDialStartInfo) func(trace.NetDialDoneInfo) {
			return func(info trace.NetDialDoneInfo) {
				if info.Error == nil {
					connectionsTotal.add(1)
				}
			}
		},
		OnNetClose: func(info trace.NetCloseStartInfo) func(trace.NetCloseDoneInfo) {
			connectionsTotal.add(-1)
			return nil
		},
	})
}
