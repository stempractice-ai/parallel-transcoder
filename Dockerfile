# syntax=docker/dockerfile:1.6
#
# Multi-stage build for the transcoder-node cluster daemon.
#
# Builder stage compiles the Rust workspace against FFmpeg 8.1 dev headers.
# Runtime stage ships only the shared libs + the two binaries we need.
#
# Built for both linux/amd64 and linux/arm64 via buildx:
#   docker buildx build --platform linux/amd64,linux/arm64 -t transcoder-node:dev .

# ---------------------------------------------------------------------------
# Stage 1a: ffmpeg-build — compile FFmpeg 8.1 from source for the target arch.
#
# We can't reuse jrottenberg/ffmpeg's prebuilt libs because that image only
# ships linux/amd64. Building from source is ~5–8 min uncached but layer-
# caches cleanly and runs natively on whatever platform docker is targeting.
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS ffmpeg-build

ARG FFMPEG_VERSION=8.1
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      autoconf \
      automake \
      build-essential \
      ca-certificates \
      cmake \
      curl \
      git \
      libtool \
      libx264-dev \
      nasm \
      pkg-config \
      yasm \
      zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
RUN curl -fsSL "https://ffmpeg.org/releases/ffmpeg-${FFMPEG_VERSION}.tar.xz" \
      | tar -xJ \
    && cd "ffmpeg-${FFMPEG_VERSION}" \
    && ./configure \
         --prefix=/usr/local \
         --enable-shared \
         --disable-static \
         --disable-doc \
         --disable-htmlpages \
         --disable-manpages \
         --disable-podpages \
         --disable-txtpages \
         --enable-gpl \
         --enable-version3 \
         --enable-libx264 \
    && make -j"$(nproc)" \
    && make install \
    && cd / && rm -rf /build

# ---------------------------------------------------------------------------
# Stage 1b: builder — FFmpeg 8.1 dev headers + rustc
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS builder

ARG RUST_VERSION=1.95.0
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      curl \
      pkg-config \
      clang \
      libssl-dev \
      libx264-dev \
    && rm -rf /var/lib/apt/lists/*

# Pull the FFmpeg artifacts we just built — same layout the upstream image
# uses (libs in /usr/local/lib, headers in /usr/local/include, binaries in
# /usr/local/bin).
COPY --from=ffmpeg-build /usr/local/lib /usr/local/lib
COPY --from=ffmpeg-build /usr/local/include /usr/local/include
COPY --from=ffmpeg-build /usr/local/bin/ffmpeg /usr/local/bin/ffmpeg
COPY --from=ffmpeg-build /usr/local/bin/ffprobe /usr/local/bin/ffprobe

ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
ENV LD_LIBRARY_PATH=/usr/local/lib

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain ${RUST_VERSION} --profile minimal
ENV PATH=/root/.cargo/bin:${PATH}

WORKDIR /src

# Prime the dependency cache — copy manifests first so `cargo fetch`
# layers cleanly on iterative source edits.
COPY Cargo.toml Cargo.lock ./
COPY coordinator/Cargo.toml coordinator/Cargo.toml
COPY worker/Cargo.toml worker/Cargo.toml
COPY cluster/Cargo.toml cluster/Cargo.toml
RUN mkdir -p coordinator/src worker/src cluster/src && \
    echo 'fn main(){}' | tee coordinator/src/main.rs worker/src/main.rs cluster/src/main.rs && \
    echo '' > cluster/src/lib.rs && \
    cargo fetch --locked

# Real source replaces the stubs; cargo rebuilds only the workspace crates.
COPY coordinator coordinator
COPY worker worker
COPY cluster cluster

RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin transcoder-node --bin transcoder-worker && \
    mkdir -p /out && \
    cp target/release/transcoder-node /out/ && \
    cp target/release/transcoder-worker /out/

# ---------------------------------------------------------------------------
# Stage 2: runtime — FFmpeg 8.1 shared libs + transcoder-node daemon
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      tini \
      libxcb1 \
      libxext6 \
      libxv1 \
      libx264-dev \
    && rm -rf /var/lib/apt/lists/*

# FFmpeg shared libs + binaries from the build stage.
COPY --from=ffmpeg-build /usr/local/lib /usr/local/lib
COPY --from=ffmpeg-build /usr/local/bin/ffmpeg /usr/local/bin/ffmpeg
COPY --from=ffmpeg-build /usr/local/bin/ffprobe /usr/local/bin/ffprobe
RUN ldconfig

# Make the FFmpeg shared libs discoverable at runtime.
ENV LD_LIBRARY_PATH=/usr/local/lib

COPY --from=builder /out/transcoder-node /usr/local/bin/transcoder-node
COPY --from=builder /out/transcoder-worker /usr/local/bin/transcoder-worker

# Non-root user. VAAPI nodes mount /dev/dri; container must be in the render
# group on the host (group id injected via K8s runAsGroup).
RUN useradd --system --home-dir /var/lib/transcoder --create-home \
      --uid 10001 --gid 0 transcoder \
    && mkdir -p /var/lib/transcoder/tmp \
    && chown -R 10001:0 /var/lib/transcoder
USER 10001

WORKDIR /var/lib/transcoder
ENV TMPDIR=/var/lib/transcoder/tmp

EXPOSE 9900
EXPOSE 9910-9930/udp

# tini is PID 1 so Rust's SIGTERM handler fires on K8s pod stop.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/transcoder-node"]
CMD ["--listen", "0.0.0.0:9900", \
     "--worker-binary", "/usr/local/bin/transcoder-worker", \
     "--lib-dir", "/usr/local/lib"]
