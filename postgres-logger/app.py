"""Consumer топика со скорами -> запись в PostgreSQL.

Читает сообщения вида {"transaction_id", "score", "fraud_flag"} из топика
скоринга и складывает их в витрину scores (см. db/init/01_scores.sql).

Семантика доставки - at-least-once:
- enable.auto.commit=false, оффсеты коммитятся только после успешной
  записи батча в БД;
- transaction_id - PRIMARY KEY, вставка через ON CONFLICT DO NOTHING,
  поэтому повторная доставка после падения безопасна.

Формат сообщений: fraud_detector пишет submission.to_json(orient="records"),
то есть JSON-МАССИВ из одного объекта. На всякий случай понимаем и плоский
объект, и батч из нескольких записей.
"""

import json
import logging
import os
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg import Connection, connect
from psycopg.errors import OperationalError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("postgres-logger")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC")
GROUP_ID = os.getenv("WRITER_GROUP_ID")

PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = int(os.getenv("POSTGRES_PORT"))
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")

BATCH_SIZE = int(os.getenv("WRITER_BATCH_SIZE"))
BATCH_TIMEOUT_S = float(os.getenv("WRITER_BATCH_TIMEOUT_S"))

INSERT_SQL = """
INSERT INTO scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
"""


def parse_records(raw: bytes) -> list[dict]:
    """Разобрать одно Kafka-сообщение в список записей скоров."""
    payload = json.loads(raw.decode("utf-8"))
    # fraud_detector шлёт orient="records": [{"score":..,"fraud_flag":..,"transaction_id":..}]
    if isinstance(payload, dict) and "records" in payload:
        payload = payload["records"]
    records = payload if isinstance(payload, list) else [payload]
    out = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        score = rec.get("score")
        fraud_flag = rec.get("fraud_flag")
        for nested_key in ("result", "data", "score_result"):
            nested = rec.get(nested_key)
            if isinstance(nested, dict):
                score = score if score is not None else nested.get("score")
                fraud_flag = (
                    fraud_flag if fraud_flag is not None else nested.get("fraud_flag")
                )
        if rec.get("transaction_id") is None or score is None or fraud_flag is None:
            logger.warning("Skip malformed record: %r", rec)
            continue
        out.append(
            {
                "transaction_id": str(rec["transaction_id"]),
                "score": float(score),
                "fraud_flag": int(fraud_flag),
            }
        )
    return out


def connect_db(retries: int = 30) -> Connection:
    """Подключение к Postgres с ожиданием готовности (db стартует дольше)."""
    for attempt in range(1, retries + 1):
        try:
            conn = connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DB,
            )
            conn.autocommit = False
            logger.info("Connected to Postgres %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
            return conn
        except OperationalError as e:
            logger.warning("Postgres not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(2)
    raise RuntimeError("Could not connect to Postgres")


def flush(conn, batch: list[dict]) -> None:
    rows = [(r["transaction_id"], r["score"], r["fraud_flag"]) for r in batch]
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
    conn.commit()


def main() -> None:
    logger.info(
        "Starting scores writer: topic=%s group=%s pg=%s:%s/%s batch=%d/%ss",
        SCORING_TOPIC,
        GROUP_ID,
        PG_HOST,
        PG_PORT,
        PG_DB,
        BATCH_SIZE,
        BATCH_TIMEOUT_S,
    )
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([SCORING_TOPIC])
    conn = connect_db()

    batch: list[dict] = []
    last_flush = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    pass
                elif err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Топик ещё не создан kafka-setup или пропали метаданные
                    # ждём, консьюмер подхватит его сам. Не падаем.
                    logger.warning("Topic not available yet: %s", err)
                    time.sleep(2)
                else:
                    logger.error("Kafka error: %s", err)
                    raise KafkaException(err)
            else:
                try:
                    batch.extend(parse_records(msg.value()))
                except Exception:
                    logger.exception("Skip undecodable message at %s", msg.topic())

            timed_out = (time.monotonic() - last_flush) >= BATCH_TIMEOUT_S
            if batch and (len(batch) >= BATCH_SIZE or timed_out):
                try:
                    flush(conn, batch)
                except Exception:
                    conn.rollback()
                    logger.exception("DB write failed, batch kept for retry")
                    time.sleep(2)
                    continue
                consumer.commit(asynchronous=False)
                logger.info("Wrote %d scores, offsets committed", len(batch))
                batch.clear()
                last_flush = time.monotonic()
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        if batch:
            try:
                flush(conn, batch)
                consumer.commit(asynchronous=False)
                logger.info("Flushed final %d scores", len(batch))
            except Exception:
                logger.exception("Failed to flush final batch")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
