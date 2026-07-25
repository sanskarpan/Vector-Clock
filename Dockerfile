# syntax=docker/dockerfile:1.7

# ── Stage 1: build ───────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS build

WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

# Copy source.
COPY . .

# Build static binary (CGO disabled, distroless-friendly).
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath \
      -ldflags="-s -w \
        -X main.version=${VERSION} \
        -X main.commit=${COMMIT}" \
      -o /out/server \
      ./cmd/server

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
# Distroless: no shell, no package manager, no CVEs in userland libs.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/server /server

USER nonroot:nonroot

# Defaults — override with -e or compose.
ENV CONFIG_PATH=/etc/vectorclock/config.yaml \
    PORT=8080 \
    LOGGING_LEVEL=info \
    LOGGING_FORMAT=json

EXPOSE 8080

# Healthcheck via wget is not available in distroless; use the application's
# /healthz endpoint via a sidecar in compose, or via Kubernetes.
HEALTHCHECK NONE

ENTRYPOINT ["/server"]