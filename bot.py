#!/usr/bin/env python3
"""CLI entrypoint for the Octocore Telegram monitor MVP."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from octocore_mvp import (
    D1Store,
    MonitorStore,
    TelegramNotifier,
    backfill,
    format_analysis,
    load_dotenv,
    monitor_websocket,
    require_public_ink_rpc,
    require_rpc_from_env,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Octocore 1/1 monitor MVP")
    parser.add_argument("command", choices=("backfill", "monitor", "analyze", "status"))
    parser.add_argument("--local-db", type=Path, help="Test-only SQLite store; production uses Cloudflare D1")
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(message)s")
    store = MonitorStore(args.local_db) if args.local_db else D1Store.from_env()
    store.migrate()

    if args.command == "analyze":
        print(format_analysis(store))
        return
    if args.command == "status":
        latest = store.latest_mint()
        state = store.next_state()
        print(f"Latest stored mint: #{latest.token_id if latest else 'N/A'}")
        print(f"Next mint: #{state.next_token_id}")
        print(f"Window: #{state.window.id} ({state.window.start}–{state.window.end})")
        print(f"Remaining: {state.remaining}")
        print(f"P(next random valid candidate = 1/1): {float(state.probability_next * 100):.4f}%")
        return

    if args.command == "backfill":
        rpc = require_rpc_from_env()
        start_block = int(os.environ.get("BACKFILL_START_BLOCK", "56504600"))
        print(f"Inserted {backfill(store, rpc, start_block)} mints")
        return

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    notifier = TelegramNotifier(token, chat_id) if token and chat_id else None
    monitor_websocket(
        store,
        require_public_ink_rpc(),
        notifier,
        confirmations=int(os.environ.get("CONFIRMATIONS", "2")),
    )


if __name__ == "__main__":
    main()
