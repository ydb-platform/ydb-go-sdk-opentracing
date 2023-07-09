## Benchmark 

Benchmark for load testing YDB with tracing.

### Installation
- Docker
  - Build image
      ```shell
      docker build -t bench . 
      ```
  - Copy docker-compose file
    ```shell
    cp ./internal/cmd/bench/native/docker-compose-example.yml ./docker-compose.yml
    ```
  - Run benchmark
    ```shell
    docker-compose up
    ```
- Binary
  - Build app
    ```shell
    go build -o ./bench ./internal/cmd/bench/native/...
    ```
  - Run benchmark
    ```shell
    ./bench
    ```

### Environment variables
| Name                    | Type     | Default    | Description                                                               |
|-------------------------|----------|------------|---------------------------------------------------------------------------|
| `SERVICE_NAME`          | `string` | ydb-go-sdk | service name on jaeger tracing                                            |
| `PREPARE_BENCH_DATA`    | `bool`   | true       | if set to false, tables will not be created and data will not be upserted |
| `WORKERS_COUNT`         | `int`    | 1          | count of workers that concurrently upsert and scan data                   |
| `BATCH_SIZE`            | `int`    | 10         | size of each batch                                                        |
| `ROWS_LEN`              | `int`    | 50         | total count of rows                                                       |
| `MAX_LIMIT`             | `int`    | 20         | max scan limit                                                            |
| `JAEGER_ENDPOINT`       | `string` |            | endpoint to jaeger-collector                                              |
| `YDB_CONNECTION_STRING` | `string` |            | connection string to YDB in format "grpc(s)://host:port/path/to/db"       |

### YDB auth environment variables

| Name                                                                                                         | Type                             | Default | yandex-cloud | Description                                                                                                                                                                                       |
|--------------------------------------------------------------------------------------------------------------|----------------------------------|---------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `YDB_ANONYMOUS_CREDENTIALS`                                                                                  | `0` or `1`                       | `0`     | `-`          | flag for use anonymous credentials                                                                                                                                                                |
| `YDB_ACCESS_TOKEN_CREDENTIALS`                                                                               | `string`                         |         | `+/-`        | use access token for authenticate with YDB. For authenticate with YDB inside yandex-cloud use short-life IAM-token. Other YDB installations can use access token depending on authenticate method |
| `YDB_STATIC_CREDENTIALS_USER`<br>`YDB_STATIC_CREDENTIALS_PASSWORD`<br>`YDB_STATIC_CREDENTIALS_ENDPOINT`<br/> | `string`<br>`string`<br>`string` |         | `-`          | static credentials from user, password and auth service endpoint                                                                                                                                  |
| `YDB_METADATA_CREDENTIALS`                                                                                   | `0` or `1`                       |         | `+`          | flag for use metadata credentials                                                                                                                                                                 |
| `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`                                                                   | `string`                         |         | `+`          | path to service account key file credentials                                                                                                                                                      |
| `YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS`                                                                        | `string`                         |         | `+`          | service account key credentials                                                                                                                                                                   |