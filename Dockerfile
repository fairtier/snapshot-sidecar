############################
# STEP 0 build arguments
############################
# Minor version on purpose, not a full patch pin: the image is only rebuilt on
# a release tag, so `1.27` resolves to the newest 1.27.x at build time rather
# than to whatever was current when someone last edited this line. Dependabot
# does not resolve an ARG-interpolated FROM, so move it by hand, with the `go`
# directive in go.mod.
ARG GO_VERSION=1.27
ARG BASE_VARIANT=trixie

############################
# STEP 1 build the binary
############################
# Pinned to the BUILD platform, not the target: CGO is off and the build below
# sets GOARCH explicitly, so Go cross-compiles the arm64 binary natively on the
# amd64 runner. Without this, buildx runs this whole stage under QEMU for the
# arm64 leg and the release takes ~10 minutes instead of ~2.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-${BASE_VARIANT} AS builder

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
