FROM brunneis/python:3.7
RUN \
    apt-get update && \
    apt-get install -y \
    git \
    build-essential \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    libgflags-dev \
    liblz4-dev \
    libzstd-dev && \
    git clone https://github.com/facebook/rocksdb.git && \
    cd rocksdb && \
    git checkout v5.18.3 && \
    DEBUG_LEVEL=0 make shared_lib install-shared && \
    cd .. && \
    rm -r rocksdb && \
    pip install --upgrade pip && \
    pip install \
    "Cython>=0.20" \
    python-rocksdb
ENV LD_LIBRARY_PATH=/usr/local/lib
ADD txlog.tar /tmp/txlog
RUN \
    cd /tmp/txlog && \
    python setup.py install && \
    cd .. && \
    rm -r txlog
