# BeatKeeper cloud service. Standard library only, so there is nothing to pip install.
FROM python:3.12-slim

ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    BEATKEEPER_CLOUD_DB=/data/cloud.db PORT=8080

RUN useradd --system --uid 10001 beatkeeper \
    && mkdir /data && chown beatkeeper /data
COPY src /app/src

USER beatkeeper
VOLUME /data
EXPOSE 8080
STOPSIGNAL SIGINT

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health' % os.environ['PORT'], timeout=2)"

# Refuse to start without a key: an unset key would leave the data open to anyone.
CMD ["sh", "-c", "[ -n \"$BEATKEEPER_API_KEY\" ] || { echo 'BEATKEEPER_API_KEY is not set' >&2; exit 1; }; exec python -m beatkeeper.cloud_server"]
