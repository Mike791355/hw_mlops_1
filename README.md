# Скоринг фродовых транзакций

Учебный проект на основе [примера с семинара](https://github.com/NikitaMalykhin/mts25_mlops_hw2_real_time_fraud_detection).
Данные — из [соревнования](https://www.kaggle.com/competitions/teta-ml-1-2025).

Сервис читает транзакции из Kafka, выполняет препроцессинг и скоринг предобученной
моделью CatBoost на CPU. Обучение модели в контейнере не выполняется.
Результаты отправляются в топик `scores` и сохраняются в PostgreSQL.
Флаг `fraud_flag` равен 1 при `score > 0.98`.

## Состав проекта

- `fraud_detector/app/app.py` — чтение и запись сообщений Kafka.
- `fraud_detector/src/preprocessing.py` — препроцессинг данных.
- `fraud_detector/src/scorer.py` — загрузка модели и скоринг.
- `interface/app.py` — загрузка CSV и просмотр результатов.
- `dbw/app.py` — запись результатов из Kafka в PostgreSQL.
- `postgres/init.sql` — создание таблицы `scores`.
- `docker-compose.yaml` — запуск всех сервисов в общей сети.

Модель находится в `fraud_detector/models/my_catboost.cbm`.
Для препроцессинга необходим `fraud_detector/train_data/train.csv.gz`:
он используется для кодирования признаков, а не для обучения модели.
Оба файла должны присутствовать перед сборкой.

## Запуск

Нужны Docker Desktop или Docker Engine с Docker Compose v2.
Порты `8501`, `8080`, `9095`, `5432` и `2181` должны быть свободны.

Откройте терминал в папке проекта и выполните:

```bash
docker compose up --build -d
docker compose ps -a
```

Дождитесь запуска сервисов. Kafka и PostgreSQL должны иметь статус `healthy`.
Контейнер `kafka-setup` после создания топиков завершает работу с кодом 0 — это нормально.
Скореру также нужно время на подготовку данных при запуске.

- Приложение: http://localhost:8501
- Kafka UI: http://localhost:8080
- PostgreSQL: `localhost:5432`, база `fraud`, пользователь и пароль `fraud`.

## Проверка работы

1. Скачайте `test.csv` соревнования. Для первого теста возьмите до 100 строк,
   сохранив заголовок и все исходные колонки.
2. Откройте приложение, загрузите CSV и нажмите кнопку «Отправить» рядом с файлом.
3. Дождитесь обработки и нажмите «Посмотреть результаты».

Приложение покажет последние 10 записей с `fraud_flag = 1` и гистограмму
скоров последних 100 транзакций. Если записей меньше 100, используются все имеющиеся.
При отсутствии результатов или фродовых транзакций выводится сообщение.
Кнопку можно нажать повторно, чтобы обновить результаты.

В Kafka UI можно просмотреть входные сообщения в `transactions` и результаты в `scores`.
Каждый результат содержит три поля:

```json
{
  "transaction_id": "example-id",
  "score": 0.9975,
  "fraud_flag": 1
}
```

Проверить количество результатов в PostgreSQL:

```bash
docker compose exec postgres psql -U fraud -d fraud -c "SELECT count(*), sum(fraud_flag) FROM scores;"
```

При ошибках можно посмотреть логи:

```bash
docker compose logs --tail 100 fraud_detector dbw
```

## Остановка

```bash
docker compose down
```

Результаты в PostgreSQL сохраняются в Docker-томе. Для остановки с удалением этих данных:

```bash
docker compose down -v
```
