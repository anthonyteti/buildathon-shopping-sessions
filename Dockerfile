# Build a static binary, then ship it alone in an empty image.
# The scorer builds natively on linux/amd64; the toolchain image is pinned by version and digest.
FROM rust:1.100.0-slim-bookworm@sha256:7c7d4eb4d05b7781187a327809466cfddf29753f6defdc3d19a0e291951b7a37 AS build
ARG TARGETARCH
# optional: limit build parallelism (unset by default)
ARG JOBS
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN set -eu; \
    if [ "${TARGETARCH:-amd64}" = "arm64" ]; then T=aarch64-unknown-linux-gnu; CPU=generic; FEAT=; \
    else T=x86_64-unknown-linux-gnu; CPU=x86-64-v3; FEAT=,+pclmulqdq; fi; \
    RUSTFLAGS="-C target-cpu=$CPU -C target-feature=+crt-static$FEAT" \
      cargo build --release --locked --target "$T" ${JOBS:+-j "$JOBS"}; \
    cp "target/$T/release/sessions" /sessions

FROM scratch
COPY --from=build /sessions /sessions
USER 1000:1000
CMD ["/sessions"]
