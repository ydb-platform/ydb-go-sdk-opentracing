package tracing

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents != 0 {
		t.OnDiscover = func(info trace.DiscoverStartInfo) func(trace.DiscoverDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_discovery",
			)
			start.SetTag("address", info.Address)
			start.SetTag("database", info.Database)
			return func(info trace.DiscoverDoneInfo) {
				finish(
					start,
					info.Error,
					"endpoints", info.Endpoints,
				)
			}
		}
	}
	return t
}
