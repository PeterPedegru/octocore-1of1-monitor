# Octocore 1/1 monitor — MVP

[English](README.md) | [Русский](README.ru.md) | 中文

这是一个运行在 Ink 上的 Octocore 链上监控工具，并通过 Telegram 发送通知。
历史回填读取 Blockscout PRO 的 `Mined` 事件，状态保存在 Cloudflare D1，
不会将 OpenSea 作为事实来源。

## MVP 功能

- 将全部 `Mined` 事件历史回填到 D1；
- 通过公共 Ink WebSocket 订阅事件，并使用 HTTPS 进行故障切换和补同步；
- 记录 1/1 历史：类型、window、minter、价格、交易和时间戳；
- 每个新确认 mint 发生后发送 Telegram 通知；
- 从已部署 implementation 恢复出的精确 1/1 条件：

```text
keccak256(seed || 0x01) % remaining == 0
```

`P(next random valid candidate)` 表示在密码学均匀随机模型下，随机有效
PoW 候选项成为 1/1 的概率。它不是“我的矿工赢得下一次 mint”的概率；如果
矿工主动丢弃普通解，它也可能不同于下一笔公开提交 mint 的概率。

## 安装与运行

```bash
python3 -m pip install -r requirements.txt
cp .env.example .env
# 仅在本地填写 .env，绝不要提交或在聊天中发送密钥。
python3 -m unittest discover -s tests -v
python3 bot.py backfill
python3 bot.py analyze
python3 -u bot.py monitor
```

`.env` 必须包含 `CLOUDFLARE_ACCOUNT_ID`、`CLOUDFLARE_API_TOKEN` 和
`CLOUDFLARE_D1_DATABASE_ID`。如需 Telegram 通知，还要设置
`TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID`。

向 Telegram bot 发送 `/start` 即可订阅 chat。bot 会用英文回复当前 window
状态。只有当前 window 的 1/1 尚未铸造时才会发送 mint alert；1/1 出现后，
通知将在下一个 window 恢复。

实时 monitor 使用公共 Ink WebSocket，不会持续消耗 Blockscout credits。
Blockscout PRO 仅用于历史回填。

## MVP 暂未包含

- 完整的 reorg rollback；
- FastAPI dashboard；
- 矿工 hashrate 与 personal streak 估算。
