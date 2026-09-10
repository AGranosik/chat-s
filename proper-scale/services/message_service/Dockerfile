# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY services/message_service ./services/message_service
COPY contracts/protos ./contracts/protos
WORKDIR /app/services/message_service
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o message_service ./cmd/server

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/services/message_service .

EXPOSE 8080
CMD ["./message_service"]
