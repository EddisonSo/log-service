FROM golang:1.24-alpine AS builder

ENV GOTOOLCHAIN=auto
WORKDIR /src

# Copy go-gfs dependency (provided by build context)
COPY go-gfs /go-gfs

# Copy log-service source
COPY log-service/go.mod log-service/go.sum /src/
COPY log-service /src/

# Update replace directive to match Docker build context paths
RUN sed -i 's|replace eddisonso.com/go-gfs => ../go-gfs|replace eddisonso.com/go-gfs => /go-gfs|' go.mod

RUN CGO_ENABLED=0 go build -o /out/log-service .

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /out/log-service /app/log-service

EXPOSE 50051 8080
ENTRYPOINT ["/app/log-service"]
