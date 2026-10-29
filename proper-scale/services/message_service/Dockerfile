# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY services/message_service ./services/message_service
COPY contracts/protos ./contracts/protos
COPY contracts/messages ./contracts/messages
WORKDIR /app/services/message_service
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o message_service ./cmd/server

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/services/message_service/message_service ./message_service
COPY --from=builder /app/services/message_service/migrations ./migrations
ENV MIGRATIONS_DIR=/app/migrations

EXPOSE 8080
CMD ["./message_service"]
