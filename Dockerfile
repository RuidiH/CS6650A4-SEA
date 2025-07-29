# Dockerfile
FROM golang:1.20-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o kv-service

FROM alpine:latest
WORKDIR /root/
COPY --from=build /app/kv-service .
EXPOSE 8000
ENTRYPOINT ["./kv-service"]