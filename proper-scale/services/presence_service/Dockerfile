# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY services/presence_service ./services/presence_service
COPY contracts/protos ./contracts/protos
WORKDIR /app/services/presence_service
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o presence_service ./cmd/server

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata


COPY --from=builder /app/services/presence_service .

EXPOSE 8080
CMD ["./presence_service"]
