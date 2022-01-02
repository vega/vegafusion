# syntax=docker/dockerfile:1
FROM quay.io/pypa/manylinux2014_x86_64:2021-07-03-d4d5413

RUN yum install -y openssl-devel

RUN curl --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

ENV PATH="$HOME/.cargo/bin:${PATH}"
ENV PATH="${PATH}:/opt/python/cp39-cp39/bin"

RUN /opt/python/cp39-cp39/bin/python -m pip install --upgrade pip maturin==0.12.3 wheel auditwheel

#RUN  curl -L "https://github.com/PyO3/maturin/releases/latest/download/maturin-x86_64-unknown-linux-musl.tar.gz" | tar -xz -C /usr/local/bin/

