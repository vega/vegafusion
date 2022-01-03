# syntax=docker/dockerfile:1
FROM quay.io/pypa/manylinux2010_x86_64:latest

RUN yum install -y openssl-devel

RUN curl --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

ENV PATH="$HOME/.cargo/bin:${PATH}"
ENV PATH="${PATH}:/opt/python/cp36-cp36m/bin:/opt/python/cp37-cp37m/bin:/opt/python/cp38-cp38/bin:/opt/python/cp39-cp39/bin"

#RUN  curl -L "https://github.com/PyO3/maturin/releases/latest/download/maturin-x86_64-unknown-linux-musl.tar.gz" | tar -xz -C /usr/local/bin/

