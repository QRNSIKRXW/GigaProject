# docker/python-runner/Dockerfile
FROM golang:1.25.5-alpine

# Устанавливаем python3
RUN apk add --no-cache python3 py3-pip

WORKDIR /app

COPY main.go .

RUN go build -o runner main.go
RUN chmod +x runner
RUN chmod 1777 /tmp

ENV LANG_TYPE=python
EXPOSE 8080

CMD ["./runner"]