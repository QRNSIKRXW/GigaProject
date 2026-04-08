FROM golang:1.21-alpine

# Устанавливаем необходимые пакеты
RUN apk add --no-cache git

WORKDIR /app

# Копируем и компилируем сервер
COPY main.go .
RUN go build -o server main.go

EXPOSE 8080

CMD ["./server"]