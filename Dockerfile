# ---------------------------
# Build Stage
# ---------------------------
FROM golang:1.24.4-alpine AS builder
WORKDIR /app

RUN apk add --no-cache git

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o binlog-es-go ./cmd/binlog-es-go


# ---------------------------
# Runtime Stage
# ---------------------------
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata wget \
 && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /app

COPY --from=builder /app/binlog-es-go .

RUN mkdir -p /data/binlog-es-go \
    /data/binlog-es-go/logs \
    /data/binlog-es-go/position

ENV BEG_CONFIG=/app/configs/config.yaml \
    BEG_MODE=realtime \
    BEG_TASK=sheet1 \
    BEG_SERVER_PORT=8222 \
    TZ=Asia/Shanghai

EXPOSE 8222

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:8222/healthz || exit 1

ENTRYPOINT ["/app/binlog-es-go"]
CMD ["--config=/app/configs/config.yaml", "--mode=realtime", "--task=sheet1"]
