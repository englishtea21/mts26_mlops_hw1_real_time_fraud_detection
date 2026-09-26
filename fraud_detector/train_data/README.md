# train_data

Эта папка пробрасывается в контейнер `fraud_detector` как volume
(`./fraud_detector/train_data:/app/train_data`).

## Что сюда положить

`train.csv` — файл из соревнования
<https://www.kaggle.com/competitions/teta-ml-1-2025>.

`preprocessing.load_train_data()` читает именно этот путь:

```python
pd.read_csv('./train_data/train.csv')   # WORKDIR контейнера = /app
```

## Требования к файлу

Нужны все колонки из `test.csv` **плюс** `target` (в `test.csv` её нет,
она нужна для mean-encoding категориалов):

```
transaction_time, merch, cat_id, amount, name_1, name_2, gender, street,
one_city, us_state, post_code, lat, lon, population_city, jobs,
merchant_lat, merchant_lon, target
```

Для быстрых тестов хватит первых ~10–50 тыс. строк: тренировочные данные
используются только для построения словарей категорий, mean-encoding
и импьютера, а не для обучения.

## После того как файл появился

```bash
docker compose restart fraud_detector
```

Сервис падает при старте (`load_train_data()` вызывается в `__init__`),
поэтому без `train.csv` в топике `scoring` не будет ничего.

Логи:

```bash
docker compose logs -f fraud_detector
```
