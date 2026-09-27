# train_data

Эта папка пробрасывается в контейнер `fraud_detector` как volume
(`./fraud_detector/train_data:/app/train_data`).

## Что сюда положить

`train.csv` - файл из соревнования
`preprocessing.load_train_data()` читает именно этот путь:

```python
pd.read_csv("./train_data/train.csv")  # WORKDIR контейнера = /app
```

## Требования к файлу

Нужны все колонки из `test.csv` **плюс** `target` (в `test.csv` её нет,
она нужна для mean-encoding категориалов):
```
