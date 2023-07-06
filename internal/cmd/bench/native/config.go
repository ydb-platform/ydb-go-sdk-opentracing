package main

import "github.com/spf13/viper"

const (
	ServiceName         = "SERVICE_NAME"
	PrepareBenchData    = "PREPARE_BENCH_DATA"
	WorkersCount        = "WORKERS_COUNT"
	RowsLen             = "ROWS_LEN"
	BatchSize           = "BATCH_SIZE"
	MaxLimit            = "MAX_LIMIT"
	JaegerEndpoint      = "JAEGER_ENDPOINT"
	YdbConnectionString = "YDB_CONNECTION_STRING"
)

func initViper() *viper.Viper {
	v := viper.New()
	v.SetDefault(ServiceName, "ydb-go-sdk")
	v.SetDefault(PrepareBenchData, true)
	v.SetDefault(WorkersCount, 1)
	v.SetDefault(RowsLen, 50)
	v.SetDefault(BatchSize, 10)
	v.SetDefault(MaxLimit, 20)
	v.SetDefault(JaegerEndpoint, "http://jaeger:14268/api/traces")

	v.AutomaticEnv()
	return v
}
