import asyncio
import logging
from prefect import flow, task
import websockets
from websockets.server import serve

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CLUSTER_TOKEN = "9612c6c1-58f7-44f1-bf6e-27534c25f88b"
TARGET_ENDPOINT = "/api/v1/metrics"
PORT = 8080

async def ws_handler(websocket):
    # Логика обработки WebSocket и VLESS туннеля (как в Node.js)
    path = websocket.path
    if path != TARGET_ENDPOINT:
        await websocket.close(code=1008, reason="Not Found")
        return
    logging.info("[Collector] Stream session established")
    # ... (обработка байтов, проверка UUID и проброс трафика)

@flow(name="main-stream-flow", log_prints=True)
async def main_stream_flow():
    logging.info(f"Запуск персистентного сервера телеметрии на порту {PORT}...")
    
    # Запускаем сервер и держим его активным бесконечно (как Node.js server.js)
    async with serve(ws_handler, "0.0.0.0", PORT):
        logging.info("Сервер успешно запущен и слушает подключения.")
        await asyncio.Future()  # Вечный асинхронный блокиратор, чтобы ран не завершался

if __name__ == "__main__":
    main_stream_flow.from_source(
        source="https://github.com/Migel-del/my-analytics-service.git",
        entrypoint="stream_deploy.py:main_stream_flow"
    ).deploy(
        name="data-stream-deployment",
        work_pool_name="wilz",
        image="prefecthq/prefect-client:3-latest"
    )
