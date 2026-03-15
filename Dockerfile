############################
# STEP 0 build arguments
############################
ARG GO_VERSION=1.26
ARG BASE_VARIANT=trixie

############################
# STEP 1 build the binary
############################
FROM golang:${GO_VERSION}-${BASE_VARIANT} AS builder

WORKDIR /app

ENV GOOS=linux \
    CGO_ENABLED=0

ARG TARGETARCH

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download && \
    go mod verify

COPY . .

RUN GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o snapshot-sidecar .

############################
# STEP 2 build a small image
############################
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /app/snapshot-sidecar /bin/snapshot-sidecar

USER nonroot:nonroot

ENTRYPOINT ["/bin/snapshot-sidecar"]
