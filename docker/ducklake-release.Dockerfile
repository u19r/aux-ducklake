# syntax=docker/dockerfile:1.28
# Versions: software-versions.snapshot.json; details in software-version-consumers.md.

FROM registry.access.redhat.com/ubi9/ubi:9.8@sha256:217dd5692d369799d34d09ffb6f5f3404efebc7900834490fdf9ae92fd6721d4 AS builder

ARG FDB_VERSION=7.4.8
ARG CROARING_VERSION=5.3.2
ARG POSTGRES_VERSION=18.6
ARG DUCKDB_PLATFORM=linux_amd64
ARG AUX_DUCKLAKE_PACKAGE_PLATFORM=linux-amd64
ARG AUX_DUCKLAKE_RELEASE_VERSION

COPY --chmod=755 scripts/install-mold.sh /usr/local/bin/install-mold.sh
ENV RUSTFLAGS="-C link-arg=-B/opt/aux-mold-3.0.0/libexec/mold -C link-arg=-fuse-ld=bfd"
ENV LDFLAGS="-B/opt/aux-mold-3.0.0/libexec/mold"
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/opt/aux-mold-3.0.0/bin/mold-cc
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/opt/aux-mold-3.0.0/bin/mold-cc

RUN set -eux; \
    arch="$(uname -m)"; \
    case "$arch" in \
        x86_64) fdb_arch="x86_64" ;; \
        aarch64) fdb_arch="aarch64" ;; \
        *) echo "unsupported UBI9 build architecture: $arch" >&2; exit 1 ;; \
    esac; \
    dnf install -y --allowerasing \
        clang \
        clang-devel \
        cmake \
        curl \
        diffutils \
        findutils \
        gcc \
        gcc-c++ \
        git \
        glibc-devel \
        gzip \
        libcurl-devel \
        make \
        ninja-build \
        openssl-devel \
        patch \
        perl \
        pkgconf-pkg-config \
        python3 \
        tar \
        unzip \
        which \
        xz; \
    dnf install -y "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients-${FDB_VERSION}-1.el9.${fdb_arch}.rpm"; \
    dnf install -y "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-server-${FDB_VERSION}-1.el9.${fdb_arch}.rpm"; \
    dnf clean all; \
    rm -rf /var/cache/dnf

RUN /usr/local/bin/install-mold.sh

RUN set -eux; \
    curl -fsSL "https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v${CROARING_VERSION}.tar.gz" | tar -xz -C /tmp; \
    cmake -G Ninja \
        -S "/tmp/CRoaring-${CROARING_VERSION}" \
        -B /tmp/croaring-build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/opt/croaring \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DROARING_BUILD_STATIC=ON \
        -DENABLE_ROARING_TESTS=OFF \
        -DENABLE_ROARING_MICROBENCHMARKS=OFF; \
    cmake --build /tmp/croaring-build --config Release; \
    cmake --install /tmp/croaring-build; \
    rm -rf "/tmp/CRoaring-${CROARING_VERSION}" /tmp/croaring-build

# UBI omits Bison and Flex; libpq does not regenerate parsers from a PostgreSQL release archive.
RUN set -eux; \
    curl -fsSL "https://ftp.postgresql.org/pub/source/v${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.tar.gz" \
        | tar -xz -C /tmp; \
    cd "/tmp/postgresql-${POSTGRES_VERSION}"; \
    BISON='/bin/echo bison version is 3.8' FLEX='/bin/echo flex 2.6.4' ./configure \
        --prefix=/opt/postgresql \
        --with-libcurl \
        --with-openssl \
        --without-icu \
        --without-readline \
        --without-zlib; \
    make -C src/include pg_config.h pg_config_os.h; \
    install -d /opt/postgresql/include/libpq; \
    install -m 0644 \
        src/include/postgres_ext.h \
        src/include/pg_config.h \
        src/include/pg_config_os.h \
        src/include/pg_config_manual.h \
        /opt/postgresql/include/; \
    install -m 0644 src/include/libpq/libpq-fs.h /opt/postgresql/include/libpq/; \
    make -C src/interfaces/libpq install; \
    make -C src/bin/pg_config install; \
    rm -rf "/tmp/postgresql-${POSTGRES_VERSION}"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:/opt/postgresql/bin:${PATH}"

ARG CURL_VERSION=8.23.0
ARG CURL_SHA256=8dadd7d2536a314f982ad05a651b713f426bf7d02cc2c76cffb9796f2218e6c5
RUN set -eux; \
    dnf install -y libnghttp2-devel zlib-devel; \
    dnf clean all; \
    curl -fsSLo /tmp/curl.tar.xz "https://github.com/curl/curl/releases/download/curl-8_23_0/curl-8.23.0.tar.xz"; \
    printf '%s  %s\n' "$CURL_SHA256" /tmp/curl.tar.xz | sha256sum -c -; \
    tar -xf /tmp/curl.tar.xz -C /tmp; \
    cmake -G Ninja -S "/tmp/curl-${CURL_VERSION}" -B /tmp/curl-build \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/curl \
        -DCMAKE_INSTALL_LIBDIR=lib -DBUILD_SHARED_LIBS=ON \
        -DBUILD_CURL_EXE=OFF -DBUILD_TESTING=OFF -DHTTP_ONLY=ON \
        -DCURL_USE_OPENSSL=ON -DCURL_USE_LIBPSL=OFF -DUSE_NGHTTP2=ON; \
    cmake --build /tmp/curl-build --parallel 4; \
    cmake --install /tmp/curl-build; \
    rm -rf /tmp/curl.tar.xz "/tmp/curl-${CURL_VERSION}" /tmp/curl-build

WORKDIR /workspace
COPY . .

ENV AUX_DUCKLAKE_SKIP_FETCH=1
ENV DUCKDB_PLATFORM=${DUCKDB_PLATFORM}
ENV GEN=ninja
ENV CC=/usr/bin/clang
ENV CXX=/usr/bin/clang++
ENV CMAKE_PREFIX_PATH=/opt/croaring
ENV AUX_DUCKLAKE_CURL_ROOT=/opt/curl
ENV PKG_CONFIG_PATH=/opt/curl/lib/pkgconfig
ENV CARGO_TARGET_DIR=/workspace/target/docker-release
ENV AUX_DUCKLAKE_RELEASE_VERSION=${AUX_DUCKLAKE_RELEASE_VERSION}

RUN --mount=type=cache,id=aux-ducklake-rust-${DUCKDB_PLATFORM},target=/workspace/target/docker-release \
    --mount=type=cache,id=aux-ducklake-cpp-clang-${DUCKDB_PLATFORM},target=/workspace/third_party/ducklake/build \
    --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    ./scripts/build_ducklake_release.sh && \
    ./scripts/package_ducklake_release.sh "${AUX_DUCKLAKE_PACKAGE_PLATFORM}" && \
    bash ./scripts/ducklake-release-container-smoke.sh "build/release/${AUX_DUCKLAKE_PACKAGE_PLATFORM}"

FROM scratch AS export
COPY --from=builder /workspace/artifacts/ /artifacts/
