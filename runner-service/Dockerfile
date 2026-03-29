FROM golang:1.25.5-alpine

WORKDIR /app

COPY . .

RUN go build -o runner .

RUN apk add --no-cache docker-cli

CMD ["./runner"]