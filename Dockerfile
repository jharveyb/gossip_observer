FROM rust:alpine AS builder
WORKDIR /app
# This is where one could build the application code as well.
RUN apk --no-cache add pkgconfig openssl-dev libc-dev
COPY ./Cargo.toml ./Cargo.lock ./
COPY ./src ./src
RUN cargo build

# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM alpine:latest
RUN apk --no-cache add ca-certificates iptables ip6tables

# Copy binary to production image.
COPY --from=builder /app/target/debug/gossip_observer /app/gossip_observer
COPY ./start.sh /app/start.sh
COPY ./config.ini /config.ini

# Copy Tailscale binaries from the tailscale image on Docker Hub.
COPY --from=docker.io/tailscale/tailscale:stable /usr/local/bin/tailscaled /app/tailscaled
COPY --from=docker.io/tailscale/tailscale:stable /usr/local/bin/tailscale /app/tailscale
RUN mkdir -p /var/run/tailscale /var/cache/tailscale /var/lib/tailscale

# Run on container startup.
CMD ["/app/start.sh"]
