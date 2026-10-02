# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY services/delivery_service ./services/delivery_service
WORKDIR /app/services/delivery_service
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o delivery_service ./cmd/server

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/services/delivery_service .

EXPOSE 8080
CMD ["./delivery_service"]
