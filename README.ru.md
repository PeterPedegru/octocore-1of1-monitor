# Octocore 1/1 monitor — MVP

[English](README.md) | Русский | [中文](README.zh-CN.md)

Ончейн-монитор Octocore в Ink с уведомлениями в Telegram. Он читает события
`Mined` из Blockscout PRO при historical backfill, хранит состояние в
Cloudflare D1 и не использует OpenSea как источник истины.

## Что делает MVP

- historical backfill всех событий `Mined` в D1;
- публичная Ink WebSocket-подписка на события с HTTPS failover и resync;
- история 1/1: тип, window, minter, цена, транзакция и timestamp;
- Telegram-уведомление после каждого нового подтверждённого mint;
- точное условие 1/1, восстановленное из deployed implementation:

```text
keccak256(seed || 0x01) % remaining == 0
```

`P(next random valid candidate)` — это вероятность для случайного валидного
PoW-кандидата в криптографической модели. Это не вероятность того, что ваш
майнер выиграет следующий mint, и она может отличаться от вероятности
следующего опубликованного mint, если майнеры отбрасывают обычные решения.

## Запуск

```bash
python3 -m pip install -r requirements.txt
cp .env.example .env
# Заполните .env локально. Никогда не отправляйте и не коммитьте секреты.
python3 -m unittest discover -s tests -v
python3 bot.py backfill
python3 bot.py analyze
python3 -u bot.py monitor
```

В `.env` нужны `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN` и
`CLOUDFLARE_D1_DATABASE_ID`. Уведомления отправляются, когда заданы также
`TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`.

Отправьте `/start` Telegram-боту, чтобы подписать chat. Бот ответит на
английском текущим статусом window. Mint alerts приходят только пока 1/1
текущего window ещё доступен; после его mint уведомления возобновятся в
следующем window.

Realtime monitor использует public Ink WebSocket и не тратит Blockscout
credits. Blockscout PRO используется только для historical backfill.

## Пока не входит в MVP

- полный rollback при reorg;
- FastAPI dashboard;
- оценки hashrate и personal streak майнера.
