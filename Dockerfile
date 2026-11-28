# syntax=docker/dockerfile:1

# Build. The cache mounts are what keep a rebuild from recompiling DataFusion;
# they need BuildKit, which has been the default for years.
FROM rust:1.98-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin ducktails \
    && cp target/release/ducktails /ducktails

# Run. `cc` rather than `static`: the glibc build is the one that will not fight
# us over TLS once object storage arrives. The `nonroot` tag sets USER for us.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /ducktails /usr/local/bin/ducktails

# Loki's port, so a Grafana datasource needs no special casing.
EXPOSE 3100

# Exec form by necessity as much as by choice: there is no shell in here, which
# also means a container healthcheck has to come from outside.
ENTRYPOINT ["/usr/local/bin/ducktails"]
CMD ["--help"]
