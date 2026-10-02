# Real-Time Fraud Detection

Система скоринга мошеннических транзакций в реальном времени: поток `CSV → Kafka → ML-модель → Kafka → PostgreSQL`, с UI для отправки транзакций и просмотра результатов.

## Сервисы

| Сервис | Папка | Что делает |
|---|---|---|
| `interface` | `./interface/` | Streamlit UI: загрузка CSV с транзакциями, отправка по одному JSON-сообщению в топик `transactions`, просмотр результатов скоринга из PostgreSQL |
| `fraud_detector` | `./fraud_detector/` (`app.py`, `src/preprocessing.py`, `src/scorer.py`) | Kafka consumer топика `transactions` → препроцессинг (время, гео-расстояние, категориальные признаки) → CatBoost-модель (`models/my_catboost.cbm`, порог 0.98) → Kafka producer в топик `scoring` |
| `postgres-logger` | `./postgres-logger/` (`app.py`) | Kafka consumer топика `scoring` → батчевая запись в PostgreSQL, таблица `scores`. At-least-once: коммит оффсетов только после записи, `ON CONFLICT DO NOTHING` по `transaction_id` |
| `db` | `./db/init/01_scores.sql` | PostgreSQL, схема витрины `scores (transaction_id (Primary Key), score, fraud_flag, created_at)` |
| `kafka`, `zookeeper`, `kafka-setup`, `kafka-ui` | `docker-compose.yaml` (образы заданы в `.env`) | Инфраструктура: брокер Kafka, автосоздание топиков `transactions` / `scoring` (3 партиции, replication 1), веб-мониторинг Kafka |

Поток данных:

```
interface → [transactions] → fraud_detector → [scoring] → postgres-logger → db (scores)
                                                    ↘ kafka-ui (мониторинг)
```

## Зависимости: uv в каждом сервисе

Каждый Python-сервис (`fraud_detector`, `interface`, `postgres-logger`) — независимый uv-проект со **своими** `pyproject.toml` и `uv.lock`:

```
fraud_detector/pyproject.toml + fraud_detector/uv.lock
interface/pyproject.toml      + interface/uv.lock
postgres-logger/pyproject.toml + postgres-logger/uv.lock
```

Общего requirements.txt в корне нет — корневой `pyproject.toml` содержит только настройки `ruff`/`mypy`.

Установка в Docker (одинаковый `Dockerfile` во всех трех сервисах, multi-stage):

1. В `builder`-стадии копируется `uv` из `ghcr.io/astral-sh/uv:latest`;
2. `uv sync --frozen --no-install-project --no-dev` по собственным `pyproject.toml` + `uv.lock` (репродуцируемая установка);
3. `.venv` копируется в финальный `python:3.13-slim` образ.

Локальная разработка одного сервиса:

```bash
cd fraud_detector  # или interface, postgres-logger
uv sync --frozen
uv run python app.py
```

Добавление зависимости:

```bash
cd <service>
uv add <package>
# коммитим обновленные pyproject.toml + uv.lock этого сервиса
```

## Запуск через Docker

В корне репозитория выполнить:

```bash
docker compose up --build
```

Конфигурация (порты, топики, креды БД) — в корневом `.env`, менять версии образов там же (`KAFKA_IMAGE`, `ZOOKEEPER_IMAGE`, `KAFKA_UI_IMAGE`, `POSTGRES_IMAGE`).

Логи:

```bash
docker compose logs -f fraud_detector
docker compose logs -f postgres-logger interface kafka
```

## Порты

Все значения по умолчанию — из `.env` (`*_PORT` — проброс на хост):

| Сервис | Хост → контейнер | Назначение |
|---|---|---|
| `interface` (Streamlit) | `8501` → `8501` (`INTERFACE_PORT`) | UI загрузки CSV и просмотра скоринга (`localhost:8501`) |
| `kafka-ui` | `8080` → `8080` (`KAFKA_UI_PORT`) | Мониторинг топиков `transactions` / `scoring`: (`localhost:8080`) |
| `kafka` | `9095` → `9092` (`KAFKA_HOST_PORT`) | Доступ к брокеру с хоста (`localhost:9095`); внутри сети compose — `kafka:9092` |
| `zookeeper` | `2181` → `2181` (`ZOOKEEPER_HOST_PORT`) | Zookeeper |
| `db` (PostgreSQL) | не проброшен, только внутри сети `ml-scorer` (`db:5432`, `PG_PORT`) | Таблица `scores`. Для доступа с хоста добавьте в `docker-compose.yaml` в сервис `db`: `ports: ["${PG_PORT}:5432"]` |
| `fraud_detector`, `postgres-logger` | портов нет | фоновые Kafka-консьюмеры, смотреть через `docker compose logs` |


## Использование

1. Загрузите CSV (`test.csv` формата соревнования) через Streamlit UI (localhost:8501, Вкладка "📤 Отправка данных").
2. `fraud_detector` пишет в топик `scoring` записи вида `{"transaction_id": "...", "score": 0.995, "fraud_flag": 1}`.
3. `postgres-logger` складывает их в таблицу `scores` — результат виден в UI (localhost:8501, Вкладка "📊 Результаты") и в `kafka-ui`.
