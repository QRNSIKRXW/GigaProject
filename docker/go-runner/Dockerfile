FROM golang:1.22-alpine

# Устанавливаем рабочую директорию
WORKDIR /code

# Устанавливаем необходимые зависимости
RUN apk add --no-cache build-base

# Контейнер будет запускать команду, которую передаст воркер:
# docker run ... gorunner go run /code/main.go
