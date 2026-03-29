FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o py-worker .

FROM debian:stable-slim

WORKDIR /app

COPY --from=builder /app/py-worker .

CMD ["./py-worker"]
