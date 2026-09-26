FROM docker.io/golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY mllib ./mllib
COPY examples/broadcastaverage ./examples/broadcastaverage
COPY cmd/gospark-worker/main.go ./cmd/gospark-worker/main.go

RUN CGO_ENABLED=0 GOOS=linux go build -o /gospark-worker ./cmd/gospark-worker/

FROM docker.io/alpine:3.19

COPY --from=builder /gospark-worker /gospark-worker

ENTRYPOINT ["/gospark-worker"]
