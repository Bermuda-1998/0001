# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.24
ARG ALPINE_VERSION=3.21
ARG XRAY_VERSION=v26.9.9

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

ARG TARGETARCH=amd64
ARG GOAMD64_LEVEL=v2

WORKDIR /src

COPY go.mod ./
COPY main.go proxy.go supervisor.go cfedge.go ./

RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) export GOARCH=amd64 GOAMD64="${GOAMD64_LEVEL}" ;; \
        arm64) export GOARCH=arm64 GOARM64=v8.0 ;; \
        *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    CGO_ENABLED=0 GOOS=linux \
    go build \
        -trimpath \
        -tags netgo,osusergo \
        -ldflags="-s -w -buildid=" \
        -o /out/bermuda-gateway .; \
    test -s /out/bermuda-gateway; \
    chmod 0555 /out/bermuda-gateway

FROM alpine:${ALPINE_VERSION} AS xray-downloader

ARG XRAY_VERSION=v26.9.9
ARG TARGETARCH=amd64

WORKDIR /tmp/xray

RUN set -eux; \
    apk add --no-cache ca-certificates wget unzip; \
    case "${TARGETARCH}" in \
        amd64) XRAY_ARCH="64" ;; \
        arm64) XRAY_ARCH="arm64-v8a" ;; \
        *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    XRAY_ZIP="Xray-linux-${XRAY_ARCH}.zip"; \
    XRAY_BASE="https://github.com/XTLS/Xray-core/releases/download/${XRAY_VERSION}"; \
    echo "Downloading Xray-core ${XRAY_VERSION} (${XRAY_ZIP})..."; \
    wget -q "${XRAY_BASE}/${XRAY_ZIP}" -O "${XRAY_ZIP}"; \
    wget -q "${XRAY_BASE}/${XRAY_ZIP}.dgst" -O "${XRAY_ZIP}.dgst"; \
    EXPECTED_SHA=$(awk -F'= *' 'toupper($1) ~ /SHA2-256|SHA256/ {gsub(/[[:space:]]/, "", $2); print $2; exit}' "${XRAY_ZIP}.dgst"); \
    printf '%s\n' "${EXPECTED_SHA}" | grep -Eq '^[0-9A-Fa-f]{64}$' || { \
        echo "FATAL: Invalid or missing SHA-256 digest in ${XRAY_ZIP}.dgst" >&2; \
        exit 1; \
    }; \
    printf '%s  %s\n' "${EXPECTED_SHA}" "${XRAY_ZIP}" | sha256sum -c -; \
    mkdir -p /out/bin /out/assets; \
    unzip -q "${XRAY_ZIP}" xray -d /out/bin; \
    unzip -q "${XRAY_ZIP}" geoip.dat geosite.dat -d /out/assets; \
    chmod 0555 /out/bin/xray; \
    chmod 0444 /out/assets/*.dat; \
    test -s /out/bin/xray; \
    test -s /out/assets/geoip.dat; \
    test -s /out/assets/geosite.dat

FROM alpine:${ALPINE_VERSION}

LABEL org.opencontainers.image.title="Edge Telemetry Gateway" \
      org.opencontainers.image.description="High-Throughput L7 Edge Stream & Metrics Gateway" \
      org.opencontainers.image.version="2.1-production" \
      org.opencontainers.image.licenses="MIT"

RUN set -eux; \
    apk add --no-cache ca-certificates tzdata wget; \
    update-ca-certificates; \
    addgroup -g 10001 -S bermuda; \
    adduser -u 10001 -S -D -H -G bermuda -h /app -s /sbin/nologin bermuda; \
    mkdir -p /app /usr/local/share/xray /usr/local/bin /tmp

COPY --from=builder --chown=root:root /out/bermuda-gateway /usr/local/bin/bermuda-gateway
COPY --from=xray-downloader --chown=root:root /out/bin/xray /usr/local/bin/xray
COPY --from=xray-downloader --chown=root:root /out/assets/geoip.dat /usr/local/share/xray/geoip.dat
COPY --from=xray-downloader --chown=root:root /out/assets/geosite.dat /usr/local/share/xray/geosite.dat
COPY --chown=root:root config.json /app/config.json

RUN set -eux; \
    chown -R root:root /app /usr/local/share/xray /usr/local/bin; \
    chmod 0555 /app /usr/local/share/xray /usr/local/bin; \
    chmod 0555 /usr/local/bin/bermuda-gateway /usr/local/bin/xray; \
    chmod 0444 /app/config.json /usr/local/share/xray/geoip.dat /usr/local/share/xray/geosite.dat; \
    chmod 1777 /tmp; \
    test -s /usr/local/bin/bermuda-gateway; \
    test -s /usr/local/bin/xray; \
    test -s /app/config.json; \
    test -s /usr/local/share/xray/geoip.dat; \
    test -s /usr/local/share/xray/geosite.dat

RUN XRAY_LOCATION_ASSET=/usr/local/share/xray \
    /usr/local/bin/xray run -test -c /app/config.json

ENV XRAY_LOCATION_ASSET=/usr/local/share/xray \
    BERMUDA_XRAY_BIN=/usr/local/bin/xray \
    BERMUDA_XRAY_CONFIG=/app/config.json \
    BERMUDA_BACKEND_XH=127.0.0.1:18443 \
    BERMUDA_BACKEND_WS=127.0.0.1:18444 \
    BERMUDA_BACKEND_TR=127.0.0.1:18445 \
    BERMUDA_PATH_XH=/api/v1/sync \
    BERMUDA_PATH_WS=/api/v1/live \
    BERMUDA_PATH_TR=/api/v1/gateway \
    BERMUDA_HEALTH_PATH=/.well-known/hc-5b1e7c \
    GODEBUG=madvdontneed=1 \
    GOGC=100 \
    TZ=UTC \
    PORT=8080

USER 10001:10001
WORKDIR /app

EXPOSE 8080
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -T 3 -O /dev/null "http://127.0.0.1:${PORT:-8080}/.well-known/hc-5b1e7c" || exit 1

CMD ["/usr/local/bin/bermuda-gateway"]
