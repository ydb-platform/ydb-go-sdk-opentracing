package main

import "github.com/spf13/viper"

const (
	ServiceName         = "SERVICE_NAME"
	PrepareBenchData    = "PREPARE_BENCH_DATA"
	WorkersCount        = "WORKERS_COUNT"
	BatchSize           = "BATCH_SIZE"
	RowsLen             = "ROWS_LEN"
	MaxLimit            = "MAX_LIMIT"
	JaegerEndpoint      = "JAEGER_ENDPOINT"
	YdbConnectionString = "YDB_CONNECTION_STRING"
)

func NewConfigByViper() *viper.Viper {
	v := viper.GetViper()
	v.SetDefault(ServiceName, "ydb-go-sdk")
	v.SetDefault(PrepareBenchData, true)
	v.SetDefault(WorkersCount, 1)
	v.SetDefault(BatchSize, 10)
	v.SetDefault(RowsLen, 50)
	v.SetDefault(MaxLimit, 20)
	v.AutomaticEnv()

	return v
}
