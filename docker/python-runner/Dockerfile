FROM python:3.12-alpine

WORKDIR /code

RUN apk add --no-cache gcc musl-dev

ENTRYPOINT ["python3"]
CMD ["/code/main.py"]
