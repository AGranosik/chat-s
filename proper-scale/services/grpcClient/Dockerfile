# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY services/grpcClient ./services/grpcClient
COPY contracts/protos ./contracts/protos
WORKDIR /app/services/grpcClient
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o grpc ./cmd/server

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata


COPY --from=builder /app/services/grpcClient .

EXPOSE 8080
CMD ["./grpc"]
