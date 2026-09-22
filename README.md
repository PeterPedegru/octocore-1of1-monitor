# Octocore 1/1 monitor — MVP

English | [Русский](README.ru.md) | [中文](README.zh-CN.md)

Telegram-first on-chain monitor for Octocore on Ink. It reads the collection's
`Mined` events from Blockscout PRO, stores state in Cloudflare D1, and never
uses OpenSea as a source of truth.

## What this MVP does

- historical D1 backfill of every `Mined` event;
- public Ink WebSocket event subscription with HTTPS failover/resync;
- 1/1 history, type, window, minter, price, transaction and timestamp;
- Telegram notification after every newly observed mint;
- exact rarity condition derived from the deployed implementation:

```text
keccak256(seed || 0x01) % remaining == 0
```

`P(next random valid candidate)` is displayed under the cryptographic-uniform
model. It is not `P(my miner wins)` and it can differ from the next submitted
mint when miners selectively discard ordinary valid solutions.

## Setup

```bash
python3 -m pip install -r requirements.txt
cp .env.example .env
# Edit .env locally. Do not paste secrets into chat or commit this file.
python3 -m unittest discover -s tests -v
python3 bot.py backfill
python3 bot.py analyze
python3 bot.py monitor
```

Set `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, and
`CLOUDFLARE_D1_DATABASE_ID` in `.env`. `monitor` sends Telegram messages only when both `TELEGRAM_BOT_TOKEN` and
`TELEGRAM_CHAT_ID` are configured. Without them it still logs newly indexed
mints as JSON lines.

## Deliberately deferred from MVP

- full reorg rollback;
- FastAPI dashboard;
- miner-hashrate and personal-streak estimates.

Realtime monitoring uses public Ink WebSocket endpoints and does not consume
Blockscout credits. Blockscout PRO is used only for the historical backfill.
The database records block hashes so reorg handling can be added without a
schema rewrite.
