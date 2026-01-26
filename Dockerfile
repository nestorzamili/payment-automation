FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99

WORKDIR /app

RUN apt-get update && apt-get install -y \
    xvfb \
    x11vnc \
    fluxbox \
    git \
    net-tools \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/novnc/noVNC.git /opt/novnc \
    && git clone --depth 1 https://github.com/novnc/websockify.git /opt/novnc/utils/websockify \
    && ln -s /opt/novnc/vnc.html /opt/novnc/index.html

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium
RUN playwright install-deps chromium

COPY . .

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 5000 6080

ENTRYPOINT ["/docker-entrypoint.sh"]
