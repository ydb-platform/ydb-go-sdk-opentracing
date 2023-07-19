FROM golang:1.20

WORKDIR /usr/src/app

ENV YDB_CA_FILE="/ydb-ca.pem"
RUN apt-get update && apt-get install -y ca-certificates && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /ydb-ca.pem

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

RUN go build -o /usr/local/bin/app ./cmd/native/...

CMD ["app"]