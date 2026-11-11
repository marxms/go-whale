# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# Images are pinned by tag and manifest-list digest. The digests were verified
# with `docker buildx imagetools inspect` on 2026-11-05.
FROM golang:1.26.10-bookworm@sha256:5de7c99eb78ca5e515a4c340235d0955ec5964a5071c492c15503d7c267da473 AS build

ARG VERSION=0.1.0
ARG COMMIT=unknown
ARG DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -p 1 -trimpath \
    -ldflags="-s -w \
      -X github.com/marxms/go-whale/internal/platform/version.Version=${VERSION} \
      -X github.com/marxms/go-whale/internal/platform/version.Commit=${COMMIT} \
      -X github.com/marxms/go-whale/internal/platform/version.Date=${DATE}" \
    -o /out/gowhale ./cmd/gowhale

FROM alpine:3.23@sha256:469c9ea410b9215eaee7976ba6c0639039b444be8e5d638f7abbada189971eb2

# The Alpine base already ships the CA bundle and busybox wget, so the runtime
# image installs no extra packages: wget serves the healthcheck and the CA
# bundle covers outbound TLS.
RUN adduser -D -H -u 10001 gowhale

COPY --from=build /out/gowhale /usr/local/bin/gowhale
COPY --chown=gowhale:gowhale config.example.yaml /etc/gowhale/config.example.yaml

USER gowhale
EXPOSE 3000

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=4 \
    CMD wget -qO- http://127.0.0.1:3000/health >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/gowhale", "serve"]
