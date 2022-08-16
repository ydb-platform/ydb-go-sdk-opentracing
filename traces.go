package tracing

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func WithTraces(details trace.Details) ydb.Option {
	return ydb.MergeOptions(
		ydb.WithTraceDriver(Driver(details)),
		ydb.WithTraceTable(Table(details)),
		ydb.WithTraceScripting(Scripting(details)),
		ydb.WithTraceScheme(Scheme(details)),
		ydb.WithTraceCoordination(Coordination(details)),
		ydb.WithTraceRatelimiter(Ratelimiter(details)),
		ydb.WithTraceDiscovery(Discovery(details)),
		ydb.WithTraceDatabaseSQL(DatabaseSQL(details)),
	)
}
