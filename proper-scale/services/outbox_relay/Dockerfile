FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY services/outbox_relay ./services/outbox_relay
COPY contracts/messages ./contracts/messages
WORKDIR /app/services/outbox_relay
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o outbox_relay ./cmd/outbox_relay

FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/services/outbox_relay/outbox_relay ./outbox_relay

CMD ["./outbox_relay"]
