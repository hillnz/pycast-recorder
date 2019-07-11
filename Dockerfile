FROM python:3.8-rc-buster AS builder

RUN pip install poetry

WORKDIR /usr/src/pycast_recorder
COPY . .

RUN poetry build -f wheel

FROM python:3.8-rc-buster

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
RUN pip install $(ls /tmp/dist/*.whl) && rm -rf /tmp/dist

RUN apk add --no-cache ffmpeg

CMD [ "python", "-m", "pycast_recorder" ]
