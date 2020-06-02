FROM golang:1.14.2 AS builder
WORKDIR /go/src/github.com/dsayan154/prometheus-remote-read-updater
COPY *.go .
COPY go.mod .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o prometheus-remote-read-updater .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /go/src/github.com/dsayan154/prometheus-remote-read-updater/prometheus-remote-read-updater .
CMD ["./prometheus-remote-read-updater"]