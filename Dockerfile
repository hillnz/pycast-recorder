FROM debian:buster AS base

RUN apt-get update && apt-get install -y \
        python3.7 \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

FROM base AS builder

RUN apt-get update && apt-get install -y \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install poetry

WORKDIR /usr/src/pycast_recorder
COPY . .

RUN poetry build -f wheel

FROM base

ENV PYCAST_SHOWS=/config/shows.yaml
ENV PYCAST_EXT=".m4a"
ENV PYCAST_FORMAT="aac"
ENV PYCAST_BITRATE="128k"
ENV PYCAST_TEMP=/config/recording
ENV PYCAST_OUT=/config/www
ENV PYCAST_PORT="80"
ENV PYCAST_HTTPBASE="http://localhost/files/"

EXPOSE 80

COPY --from=builder /usr/src/pycast_recorder/dist /tmp/dist
RUN pip3 install $(ls /tmp/dist/*.whl) && rm -rf /tmp/dist

RUN apt-get update && apt-get install -y \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*

CMD [ "python3", "-m", "pycast_recorder" ]
