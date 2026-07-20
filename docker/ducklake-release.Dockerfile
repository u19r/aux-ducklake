# syntax=docker/dockerfile:1.7

FROM registry.access.redhat.com/ubi9/ubi:latest AS builder

ARG FDB_VERSION=7.4.6
ARG CROARING_VERSION=4.3.12
ARG POSTGRES_VERSION=18.3
ARG DUCKDB_PLATFORM=linux_amd64
ARG AUX_DUCKLAKE_PACKAGE_PLATFORM=linux-amd64
ARG AUX_DUCKLAKE_RELEASE_VERSION

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
    dnf clean all; \
    rm -rf /var/cache/dnf

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

WORKDIR /workspace
COPY . .

ENV AUX_DUCKLAKE_SKIP_FETCH=1
ENV DUCKDB_PLATFORM=${DUCKDB_PLATFORM}
ENV GEN=ninja
ENV CMAKE_PREFIX_PATH=/opt/croaring
ENV CARGO_TARGET_DIR=/workspace/target/docker-release
ENV AUX_DUCKLAKE_RELEASE_VERSION=${AUX_DUCKLAKE_RELEASE_VERSION}

RUN ./scripts/build_ducklake_release.sh
RUN ./scripts/package_ducklake_release.sh "${AUX_DUCKLAKE_PACKAGE_PLATFORM}"

FROM scratch AS export
COPY --from=builder /workspace/artifacts/ /artifacts/
