# runner-service/Dockerfile
FROM golang:1.25.5-alpine

# Устанавливаем Docker CLI
RUN apk add --no-cache docker-cli

WORKDIR /app

# Копируем go.mod и go.sum
COPY go.mod go.sum ./
RUN go mod download

# Копируем исходный код
COPY . .

# Собираем приложение
RUN go build -o runner-service main.go

EXPOSE 9000

CMD ["./runner-service"]