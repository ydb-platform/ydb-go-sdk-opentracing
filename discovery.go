package tracing

import (
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents != 0 {
		t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(discovery trace.DiscoveryDiscoverDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_discovery",
			)
			start.SetTag("address", info.Address)
			start.SetTag("database", info.Database)
			return func(info trace.DiscoveryDiscoverDoneInfo) {
				finish(
					start,
					info.Error,
					otlog.Object("endpoints", info.Endpoints),
				)
			}
		}
	}
	return t
}
