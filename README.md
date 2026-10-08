# Real-Time Fraud Detection System

Сервис скоринга мошеннических транзакций в реальном времени. Streamlit-интерфейс отправляет транзакции в Kafka, сервис `fraud_detector` читает их, делает препроцессинг, применяет CatBoost-модель (inference на CPU) и пишет скор и флаг фрода в Kafka.

## Компоненты

- `interface` — Streamlit UI: загружает CSV и отправляет транзакции JSON-сообщениями в топик `transactions`.
- `fraud_detector` — ML-сервис: читает `transactions`, делает препроцессинг, скорит моделью `fraud_detector/models/my_catboost.cbm` и пишет результат в топик `scoring`.
- Kafka-инфраструктура: Zookeeper, Kafka, `kafka-setup` (создаёт топики), Kafka UI.

## Требования

- Docker и Docker Compose
- Свободные порты: 8080, 8501, 9095

## Запуск

1. Клонировать репозиторий:
```
   git clone https://github.com/ksbatalova/fraud-detection-mlops.git
   cd fraud-detection-mlops
```
2. Скачать данные соревнования https://www.kaggle.com/competitions/teta-ml-1-2025 (вкладка Data): `train.csv` и `test.csv`.
3. Создать папку `fraud_detector/train_data` и положить в неё `train.csv` (путь `fraud_detector/train_data/train.csv`). В репозитории файла нет из-за размера.
4. Собрать и запустить контейнеры:
```
   docker compose up --build
```
5. Подождать, пока в логах `fraud_detector` появится строка `Train data processed`.

## Использование

1. Открыть Streamlit: http://localhost:8501 и загрузить CSV формата `test.csv`. Для первой проверки лучше взять небольшой кусок (до 100 строк), чтобы не ждать долго.
2. Открыть Kafka UI: http://localhost:8080 → Topics → `transactions` (входные данные) и `scoring` (результаты).

Формат сообщения в `scoring`:

```
{"score": 0.995, "fraud_flag": 1, "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"}
```

## Остановка

```
docker compose down
```