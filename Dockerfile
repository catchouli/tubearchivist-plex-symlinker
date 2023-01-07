FROM rust:1.66.0

WORKDIR /usr/src/tubearchivist-plex-symlinker
COPY . .

RUN cargo install --path .

CMD ["tubearchivist-plex-symlinker"]
