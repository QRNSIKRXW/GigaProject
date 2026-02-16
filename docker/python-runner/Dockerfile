FROM python:3.12-alpine

WORKDIR /code

# Устанавливаем зависимости, которые могут понадобиться пользователю
RUN apk add --no-cache gcc musl-dev

# Никакого CMD — воркер сам передаёт команду:
# docker run ... pyrunner python /code/main.py
