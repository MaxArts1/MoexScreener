# MOEX Screener v3.1 — Docker image
# Build: docker build -t moex-screener .
# Run:   docker run --rm -p 8080:8080 -v $(pwd)/robot_db:/app/robot_db moex-screener
#        then open http://localhost:8080/dashboard.html

FROM python:3.13-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    TZ=Europe/Moscow \
    ROBOT_DB_DIR=/app/robot_db

# Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application + dashboard
COPY moex_screener_v3_1.py server.py dashboard.html ./
COPY .env.example ./

# Mount points / writable dirs
RUN mkdir -p /app/robot_db /app/data
VOLUME ["/app/robot_db"]

# Dashboard (dashboard.html + data/screener.json) is served here
EXPOSE 8080

ENTRYPOINT ["python", "moex_screener_v3_1.py"]
CMD ["--rest", "--http", "--port", "8080"]
