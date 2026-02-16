FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o service .

FROM debian:stable-slim

WORKDIR /app

COPY --from=builder /app/service .

CMD ["./service"]
