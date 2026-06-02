FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home /app tread \
    && mkdir -p /app /config \
    && chown -R tread:tread /app /config
WORKDIR /app
COPY --from=builder /app/target/release/tread /usr/local/bin/tread
USER tread
EXPOSE 80
CMD ["tread", "serve"]
