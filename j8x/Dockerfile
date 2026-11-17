# j8x add-on — taban imaj burada (build.yaml Supervisor 2026.11 ile kullanımdan kalktı).
# Supervisor BUILD_ARCH'ı --build-arg ile verir; hazır imaj scripts/addon_imaj_yayinla.sh ile Mac'te derlenip registry'ye itilir.
ARG BUILD_ARCH=aarch64
FROM ghcr.io/home-assistant/${BUILD_ARCH}-base-python:3.12-alpine3.22
ARG BUILD_ARCH
ARG BUILD_VERSION=dev
# paho-mqtt + aiohttp: aarch64/amd64 musl icin hazir wheel var (PyPI'da dogrulandi 2026-11-16); derleme yok
RUN pip3 install --no-cache-dir "paho-mqtt>=2,<3" "aiohttp>=3.9,<4" tzdata
COPY j8b_agent.py /app/j8b_agent.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh
# Supervisor'un hazir imajda aradigi etiketler
LABEL io.hass.name="j8b" io.hass.description="j8b bulut ajani" io.hass.type="addon" io.hass.arch="${BUILD_ARCH}" io.hass.version="${BUILD_VERSION}" org.opencontainers.image.source="https://github.com/sezaicetin/j8x-addons"
CMD [ "/run.sh" ]
