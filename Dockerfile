FROM rust:alpine AS builder
WORKDIR /app
# This is where one could build the application code as well.
RUN apk add pkgconfig openssl-dev libc-dev
COPY ./Cargo.toml ./Cargo.lock ./
COPY ./src ./src
COPY ./start.sh ./
COPY ./config.ini ./
RUN cargo build

# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM alpine:latest
RUN apk update && apk add ca-certificates iptables ip6tables && rm -rf /var/cache/apk/*

# Copy binary to production image.
COPY --from=builder /app/target/debug/gossip_observer /app/gossip_observer
COPY --from=builder /app/start.sh /app/start.sh
COPY --from=builder /app/config.ini /config.ini

# Copy Tailscale binaries from the tailscale image on Docker Hub.
COPY --from=docker.io/tailscale/tailscale:stable /usr/local/bin/tailscaled /app/tailscaled
COPY --from=docker.io/tailscale/tailscale:stable /usr/local/bin/tailscale /app/tailscale
RUN mkdir -p /var/run/tailscale /var/cache/tailscale /var/lib/tailscalIe

# Run on container startup.
CMD ["/app/start.sh"]
