<p align="center">
  <img src="assets/logo.png" alt="Логотип Aviso" width="240">
</p>

# Aviso — Notification Service

Сервис асинхронной отправки уведомлений в Telegram на Go.
Внешнее приложение создаёт уведомление через REST API, а отдельный worker отправляет его и сохраняет результат обработки.

## Стек

Go 1.26, PostgreSQL 16, Redis Streams, Telegram Bot API,
Docker Compose, pgx, go-redis, Goose.

## Возможности

- Создание уведомления и получение его состояния по HTTP.
- Transactional Outbox: уведомление и событие создаются одной транзакцией PostgreSQL.
- Фоновая публикация задач в Redis Streams.
- Обработка через consumer group с подтверждением XACK.
- До пяти попыток отправки с задержками 1/3/9/27 секунд.
- Классификация ошибок Telegram и учёт `retry_after`.
- Хранение статусов, ошибок и истории попыток.
- Unit-тесты и интеграционные тесты с PostgreSQL и Redis.

## Архитектура

```text
HTTP API → PostgreSQL: notifications + outbox
                          ↓
                   Outbox Publisher
                          ↓
                     Redis Streams
                          ↓
                        Worker → Telegram Bot API
                          ↓
                   PostgreSQL → XACK
```

API возвращает `202 Accepted` после сохранения уведомления,
не дожидаясь отправки в Telegram.

Внешнее приложение передаёт готовый `chat_id` в поле `recipient`.
Пользователи и их контактные данные остаются во внешнем приложении.

## Локальный запуск

Нужны Go 1.26+, Docker Compose, Make и Goose с поддержкой PostgreSQL.

Создайте `.env` в корне проекта:

```dotenv
HTTP_ADDR=:8080
DATABASE_URL=postgres://notification:notification_local@localhost:5433/notifications?sslmode=disable
REDIS_ADDR=127.0.0.1:6380
TELEGRAM_BOT_TOKEN=YOUR_BOT_TOKEN

GOOSE_DRIVER=postgres
GOOSE_DBSTRING=postgres://notification:notification_local@localhost:5433/notifications?sslmode=disable
```

Токен выдаётся через BotFather. Перед отправкой в личный чат
пользователь должен написать боту. В запросе используется ID
этого чата, а не ID бота. Не добавляйте `.env` в Git.

Запустите PostgreSQL и Redis:

```bash
make up
docker compose ps
```

Дождитесь статуса `healthy` у обоих сервисов, затем примените миграции:

```bash
make migrate-up
```

В двух отдельных терминалах запустите:

```bash
make run-api
```

```bash
make run-worker
```

Makefile загружает переменные из `.env`.
Docker Compose сейчас запускает только PostgreSQL и Redis;
API и worker работают на хосте.

## API

| Метод | Путь                  | Назначение                                |
| ----- | --------------------- | ----------------------------------------- |
| POST  | `/notifications`      | Создать уведомление                       |
| GET   | `/notifications/{id}` | Получить уведомление и статус             |
| GET   | `/health`             | Проверить работоспособность HTTP-процесса |

Пример создания — замените `123456789` на свой `chat_id`:

```bash
curl -i http://localhost:8080/notifications \
  -H 'Content-Type: application/json' \
  -d '{"recipient":"123456789","channel":"telegram","message":"Hello from Aviso!"}'
```

Ответ: `202 Accepted`

```json
{
  "id": 1,
  "status": "pending"
}
```

Проверка состояния:

```bash
curl http://localhost:8080/notifications/1
```

Статусы: `pending`, `processing`, `sent`, `failed`.

`sent` означает, что Telegram API принял сообщение.
Это не подтверждение его прочтения пользователем.
`/health` проверяет HTTP-процесс, а не доступность зависимостей.

## Тесты

```bash
make test GOFLAGS="-count=1 -v"
```

Для интеграционных тестов должны работать PostgreSQL и Redis
и быть заданы `DATABASE_URL` и `REDIS_ADDR`.
Без соответствующих переменных эти тесты пропускаются.
На время интеграционных тестов остановите worker приложения.

## Текущие ограничения

- Поддерживается один активный worker и один Telegram-бот на экземпляр сервиса.
- Exactly-once отправка не гарантируется: при сбое между принятием
  сообщения Telegram и сохранением результата возможен дубль.
- Автоматическое восстановление PEL ещё не подключено к worker.
  Redis-адаптер XAUTOCLAIM и метод восстановления попытки в PostgreSQL
  реализованы и протестированы отдельно.
- Восстановление зависших задач, завершение работы по сигналу
  и контейнеризация приложений находятся в разработке.
