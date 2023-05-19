# renovate: datasource=docker depName=python
ARG PYTHON_VERSION=3.11.3
FROM --platform=$BUILDPLATFORM python:${PYTHON_VERSION} AS builder

# renovate: datasource=pypi depName=poetry
ARG POETRY_VERSION=1.5.0
RUN pip install poetry==${POETRY_VERSION} --prefer-binary

WORKDIR /usr/src/pycast_recorder
COPY . .

RUN poetry build -f wheel

FROM python:${PYTHON_VERSION}

ENV PYCAST_SHOWS=/config/shows.yaml
ENV PYCAST_EXT=".m4a"
ENV PYCAST_CODEC="aac"
ENV PYCAST_BITRATE="128k"
ENV PYCAST_TEMP=/config/recording
ENV PYCAST_OUT=/config/www
ENV PYCAST_PORT="80"
ENV PYCAST_HTTPBASE="http://localhost/files/"

EXPOSE 80

RUN apt-get update && apt-get install -y \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/pycast_recorder/dist /tmp/dist
RUN pip install $(ls /tmp/dist/*.whl) && rm -rf /tmp/dist

CMD [ "python", "-m", "pycast_recorder" ]
