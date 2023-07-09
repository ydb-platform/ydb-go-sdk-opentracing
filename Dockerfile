FROM golang:1.20

WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

RUN go build -o /usr/local/bin/app ./internal/cmd/bench/native/...

CMD ["app"]