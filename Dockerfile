FROM golang:1.23.2-bullseye

USER root

RUN apt-get update && \
   DEBIAN_FRONTEND=noninteractive apt-get \
   -y --allow-downgrades --allow-remove-essential --allow-change-held-packages \
   install bash build-essential cmake git

RUN git clone https://github.com/duckdb/duckdb.git /duckdb

WORKDIR /duckdb/build

RUN cmake .. && make -j6 && make install

ENV LD_LIBRARY_PATH=/duckdb/build/src

WORKDIR /app