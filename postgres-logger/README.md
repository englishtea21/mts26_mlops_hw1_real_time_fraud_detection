# postgres-logger

Consumer топика со скорами → запись в витрину `scores` в PostgreSQL.

## Контракт

- Вход: топик `${SCORING_TOPIC}` (по умолчанию `scoring`), сообщения
  `[{"score": float, "fraud_flag": 0|1, "transaction_id": str}]` -
  именно так пишет `fraud_detector` (`to_json(orient="records")`).
  Понимает и плоский объект, и батч.
- Выход: таблица `scores` (`db/init/01_scores.sql`).
