FROM brunneis/python:3.7

RUN \
    apt-get update && apt-get -y upgrade \
    && dpkg-query -Wf '${Package}\n' | sort > init_pkgs \
    && apt-get install -y \
        build-essential

RUN \
    apt-get install -y \
        libgflags-dev \
        libsnappy-dev \
        zlib1g-dev \
        libbz2-dev \
        liblz4-dev \
        libzstd-dev \
        git \
    && dpkg-query -Wf '${Package}\n' | sort > new_pkgs \
    && git clone https://github.com/facebook/rocksdb.git \
    && cd rocksdb \
    && make static_lib \
    && make install \
    && make install-shared INSTALL_PATH=/usr

RUN \
    pip install --upgrade pip \
    && pip install \
        "Cython>=0.20" \
        python-rocksdb

RUN \
    apt-get -y purge $(diff -u init_pkgs new_pkgs | grep -E "^\+" | cut -d + -f2- | sed -n '1!p') \
    && apt-get clean \
    && rm init_pkgs new_pkgs
