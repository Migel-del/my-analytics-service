import asyncio
import logging
from prefect import flow, task

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@task(name="process-stream-data")
async def process_data_stream():
    logging.info("Запуск обработки потока данных...")
    try:
        await asyncio.sleep(2)
        logging.info("Данные успешно получены и обработаны.")
    except Exception as e:
        logging.info(f"Соединение с клиентом завершено: {e}")


@flow(name="main-stream-flow", log_prints=True)
async def main_stream_flow():
    logging.info("Старт главного потока...")
    await process_data_stream()
    logging.info("Поток успешно завершил работу.")


if __name__ == "__main__":
    # Используем .from_source для привязки удаленного репозитория GitHub
    main_stream_flow.from_source(
        source="https://github.com/Migel-del/my-analytics-service.git",
        entrypoint="stream_deploy.py:main_stream_flow"
    ).deploy(
        name="data-stream-deployment",
        work_pool_name="wilz",
        image="prefecthq/prefect-client:3-latest"
    )
