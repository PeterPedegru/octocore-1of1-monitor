"""On-chain Octocore 1/1 monitor MVP.

The rarity rule was reconstructed from the deployed bytecode and verified against
all 1/1 Mined events available at launch:
    keccak256(seed || 0x01) % remaining == 0
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from html import escape
from dataclasses import dataclass
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable, Optional

import websocket


CHAIN_ID = 57073
COLLECTION_ADDRESS = "0x2BFD28383996A0717f50819d1A51Bd2Cea6F929d"
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
MAX_SUPPLY = 11_111
WINDOW_SIZE = 694
UNIQUE_TOTAL = 16
UNIQUE_SENTINEL = 255
MINED_TOPIC = "0xc55e0b0a04e8540e1f0182c2bb9f91e4a81748234b82abfe64ea7eec73a2d520"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
HASH_SPACE = 2**256
PUBLIC_HTTPS_RPCS = ("https://rpc-gel.inkonchain.com", "https://rpc-ten.inkonchain.com", "https://ink.drpc.org")
PUBLIC_WS_RPCS = ("wss://ws-gel.inkonchain.com", "wss://rpc-ten.inkonchain.com", "wss://ink.drpc.org")

UNIQUE_TYPES = (
    "Ink Blob",
    "Davy Jones",
    "Squidward",
    "Cosmic",
    "Pepe",
    "McDonalds",
    "Cthulhu",
    "Matrix",
    "Skeleton",
    "Ghost",
    "Gold",
    "Oswald",
    "Ninja",
    "Diamond",
    "Quotron's Machine",
    "Mummy",
)


@dataclass(frozen=True)
class Window:
    id: int
    start: int
    end: int

    @property
    def size(self) -> int:
        return self.end - self.start + 1

    def position(self, token_id: int) -> int:
        if not self.start <= token_id <= self.end:
            raise ValueError(f"token {token_id} is outside window {self.id}")
        return token_id - self.start + 1


def window_for_token(token_id: int) -> Window:
    if not 1 <= token_id <= MAX_SUPPLY:
        raise ValueError(f"token_id must be between 1 and {MAX_SUPPLY}")
    # Windows 1–15 contain 694 tokens. Window 16 starts at 15 * 694 + 1
    # and absorbs both the 16th 694-token segment and the seven-token tail.
    window_id = min((token_id - 1) // WINDOW_SIZE + 1, UNIQUE_TOTAL)
    start = (window_id - 1) * WINDOW_SIZE + 1
    end = MAX_SUPPLY if window_id == UNIQUE_TOTAL else window_id * WINDOW_SIZE
    return Window(id=window_id, start=start, end=end)


def unique_type(unique_index: int) -> Optional[str]:
    if unique_index == UNIQUE_SENTINEL:
        return None
    if not 0 <= unique_index < len(UNIQUE_TYPES):
        return f"UNKNOWN_INDEX_{unique_index}"
    return UNIQUE_TYPES[unique_index]


def mint_price_wei(token_id: int) -> int:
    """Contract priceOf(tokenId): 0.01 ETH times the one-based epoch."""
    epoch = min((token_id - 1) // 1_111, 9)
    return (epoch + 1) * 10**16


def candidate_probability(remaining: int) -> Fraction:
    """Exact model for a uniformly distributed 256-bit rarity draw.

    Solidity tests hash % remaining == 0. Residue zero has ceil(2^256 / R)
    preimages, rather than exactly 2^256 / R when R does not divide 2^256.
    """
    if remaining < 1:
        return Fraction(0, 1)
    return Fraction((HASH_SPACE + remaining - 1) // remaining, HASH_SPACE)


def probability_within(remaining: int, mint_count: int, unique_already_minted: bool = False) -> Fraction:
    if unique_already_minted or remaining < 1 or mint_count < 1:
        return Fraction(0, 1)
    probability_no_unique = Fraction(1, 1)
    for offset in range(min(remaining, mint_count)):
        probability_no_unique *= 1 - candidate_probability(remaining - offset)
    return 1 - probability_no_unique


@dataclass(frozen=True)
class NextMintState:
    window: Window
    next_token_id: int
    position: int
    remaining: int
    current_window_has_unique: bool
    probability_next: Fraction


def state_for_next_mint(latest_minted: int, current_window_has_unique: bool) -> NextMintState:
    if latest_minted < 0 or latest_minted > MAX_SUPPLY:
        raise ValueError("latest_minted is outside collection bounds")
    if latest_minted == MAX_SUPPLY:
        last_window = window_for_token(MAX_SUPPLY)
        return NextMintState(last_window, MAX_SUPPLY + 1, last_window.size, 0, True, Fraction(0, 1))

    next_token_id = latest_minted + 1
    window = window_for_token(next_token_id)
    if latest_minted == 0 or next_token_id == window.start:
        position = 0
        remaining = window.size
        has_unique = False
    else:
        position = window.position(latest_minted)
        remaining = window.end - latest_minted
        has_unique = current_window_has_unique
    return NextMintState(
        window=window,
        next_token_id=next_token_id,
        position=position,
        remaining=remaining,
        current_window_has_unique=has_unique,
        probability_next=Fraction(0, 1) if has_unique else candidate_probability(remaining),
    )


@dataclass(frozen=True)
class MintEvent:
    token_id: int
    tx_hash: str
    block_number: int
    block_hash: str
    timestamp: int
    minter: str
    price_wei: int
    seed: str
    work: str
    target: str
    nonce: str
    unique_index: int

    @property
    def window(self) -> Window:
        return window_for_token(self.token_id)

    @property
    def position(self) -> int:
        return self.window.position(self.token_id)

    @property
    def is_unique(self) -> bool:
        return self.unique_index != UNIQUE_SENTINEL


@dataclass(frozen=True)
class StoredMint:
    token_id: int
    unique_index: int
    window_id: int
    position: int
    block_number: int
    timestamp: int
    tx_hash: str
    minter: str


class MonitorStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def migrate(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS blocks (
                    block_number INTEGER PRIMARY KEY,
                    block_hash TEXT NOT NULL,
                    timestamp INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS windows (
                    window_id INTEGER PRIMARY KEY,
                    start_token_id INTEGER NOT NULL,
                    end_token_id INTEGER NOT NULL,
                    one_of_one_token_id INTEGER,
                    one_of_one_minted INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS mints (
                    token_id INTEGER PRIMARY KEY,
                    tx_hash TEXT NOT NULL UNIQUE,
                    block_number INTEGER NOT NULL,
                    block_hash TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    minter TEXT NOT NULL,
                    price_wei TEXT NOT NULL,
                    seed TEXT NOT NULL,
                    work TEXT NOT NULL,
                    target TEXT NOT NULL,
                    nonce TEXT NOT NULL,
                    window_id INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    is_1of1 INTEGER NOT NULL,
                    rarity_type TEXT,
                    unique_index INTEGER NOT NULL,
                    FOREIGN KEY(block_number) REFERENCES blocks(block_number),
                    FOREIGN KEY(window_id) REFERENCES windows(window_id)
                );
                CREATE TABLE IF NOT EXISTS uniques (
                    token_id INTEGER PRIMARY KEY,
                    window_id INTEGER NOT NULL UNIQUE,
                    type TEXT NOT NULL,
                    unique_index INTEGER NOT NULL,
                    block_number INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    tx_hash TEXT NOT NULL UNIQUE,
                    minter TEXT NOT NULL,
                    position INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS telegram_subscribers (
                    chat_id TEXT PRIMARY KEY,
                    username TEXT,
                    subscribed_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_mints_block ON mints(block_number, token_id);
                """
            )

    def insert_mint(self, mint: MintEvent) -> bool:
        window = mint.window
        rarity = unique_type(mint.unique_index)
        with self._connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO blocks(block_number, block_hash, timestamp) VALUES (?, ?, ?)",
                (mint.block_number, mint.block_hash, mint.timestamp),
            )
            db.execute(
                """INSERT OR IGNORE INTO windows(window_id, start_token_id, end_token_id)
                   VALUES (?, ?, ?)""",
                (window.id, window.start, window.end),
            )
            inserted = db.execute(
                """INSERT OR IGNORE INTO mints(
                    token_id, tx_hash, block_number, block_hash, timestamp, minter, price_wei,
                    seed, work, target, nonce, window_id, position, is_1of1, rarity_type, unique_index
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    mint.token_id,
                    mint.tx_hash,
                    mint.block_number,
                    mint.block_hash,
                    mint.timestamp,
                    mint.minter.lower(),
                    str(mint.price_wei),
                    mint.seed,
                    mint.work,
                    mint.target,
                    mint.nonce,
                    window.id,
                    mint.position,
                    int(mint.is_unique),
                    rarity,
                    mint.unique_index,
                ),
            ).rowcount == 1
            if inserted and mint.is_unique:
                db.execute(
                    """UPDATE windows SET one_of_one_token_id = ?, one_of_one_minted = 1
                       WHERE window_id = ?""",
                    (mint.token_id, window.id),
                )
                db.execute(
                    """INSERT OR IGNORE INTO uniques(
                        token_id, window_id, type, unique_index, block_number, timestamp, tx_hash, minter, position
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        mint.token_id,
                        window.id,
                        rarity or "UNKNOWN",
                        mint.unique_index,
                        mint.block_number,
                        mint.timestamp,
                        mint.tx_hash,
                        mint.minter.lower(),
                        mint.position,
                    ),
                )
            return inserted

    def mint_count(self) -> int:
        with self._connect() as db:
            return int(db.execute("SELECT COUNT(*) FROM mints").fetchone()[0])

    def latest_mint(self) -> Optional[StoredMint]:
        with self._connect() as db:
            row = db.execute(
                """SELECT token_id, unique_index, window_id, position, block_number, timestamp, tx_hash, minter
                   FROM mints ORDER BY token_id DESC LIMIT 1"""
            ).fetchone()
        return self._stored_mint(row)

    def last_unique(self) -> Optional[StoredMint]:
        with self._connect() as db:
            row = db.execute(
                """SELECT token_id, unique_index, window_id, position, block_number, timestamp, tx_hash, minter
                   FROM mints WHERE is_1of1 = 1 ORDER BY token_id DESC LIMIT 1"""
            ).fetchone()
        return self._stored_mint(row)

    @staticmethod
    def _stored_mint(row: Optional[sqlite3.Row]) -> Optional[StoredMint]:
        if row is None:
            return None
        return StoredMint(**dict(row))

    def window_has_unique(self, window_id: int) -> bool:
        with self._connect() as db:
            row = db.execute("SELECT one_of_one_minted FROM windows WHERE window_id = ?", (window_id,)).fetchone()
        return bool(row and row[0])

    def unique_for_window(self, window_id: int) -> Optional[StoredMint]:
        with self._connect() as db:
            row = db.execute(
                """SELECT token_id, unique_index, window_id, position, block_number, timestamp, tx_hash, minter
                   FROM mints WHERE window_id = ? AND is_1of1 = 1 LIMIT 1""",
                (window_id,),
            ).fetchone()
        return self._stored_mint(row)

    def next_state(self) -> NextMintState:
        latest = self.latest_mint()
        if latest is None:
            return state_for_next_mint(0, False)
        current_window = window_for_token(latest.token_id)
        return state_for_next_mint(latest.token_id, self.window_has_unique(current_window.id))

    def mints_since_last_unique(self) -> Optional[int]:
        latest = self.latest_mint()
        unique = self.last_unique()
        if latest is None or unique is None:
            return None
        return latest.token_id - unique.token_id

    def get_sync_block(self) -> Optional[int]:
        with self._connect() as db:
            row = db.execute("SELECT value FROM sync_state WHERE key = 'last_scanned_block'").fetchone()
        return int(row[0]) if row else None

    def set_sync_block(self, block_number: int) -> None:
        with self._connect() as db:
            db.execute(
                """INSERT INTO sync_state(key, value) VALUES ('last_scanned_block', ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
                (str(block_number),),
            )

    def get_telegram_update_offset(self) -> Optional[int]:
        with self._connect() as db:
            row = db.execute("SELECT value FROM sync_state WHERE key = 'telegram_update_offset'").fetchone()
        return int(row[0]) if row else None

    def set_telegram_update_offset(self, offset: int) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO sync_state(key, value) VALUES ('telegram_update_offset', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (str(offset),))

    def unique_rows(self) -> list[sqlite3.Row]:
        with self._connect() as db:
            return db.execute("SELECT * FROM uniques ORDER BY window_id").fetchall()

    def add_subscriber(self, chat_id: str, username: Optional[str] = None) -> None:
        with self._connect() as db:
            db.execute("INSERT OR IGNORE INTO telegram_subscribers(chat_id, username, subscribed_at) VALUES (?, ?, ?)", (str(chat_id), username, int(time.time())))

    def subscriber_chat_ids(self) -> list[str]:
        with self._connect() as db:
            return [row[0] for row in db.execute("SELECT chat_id FROM telegram_subscribers ORDER BY subscribed_at")]


class D1Store:
    """Production store backed by the Cloudflare D1 HTTP API."""

    def __init__(self, account_id: str, api_token: str, database_id: str, timeout_seconds: int = 30) -> None:
        if not account_id or not api_token or not database_id:
            raise ValueError("Cloudflare account ID, API token, and D1 database ID are required")
        self.account_id = account_id
        self.api_token = api_token
        self.database_id = database_id
        self.timeout_seconds = timeout_seconds
        self._ssl_context = trusted_ssl_context()

    @classmethod
    def from_env(cls) -> "D1Store":
        return cls(
            os.environ.get("CLOUDFLARE_ACCOUNT_ID", ""),
            os.environ.get("CLOUDFLARE_API_TOKEN", ""),
            os.environ.get("CLOUDFLARE_D1_DATABASE_ID", ""),
        )

    def _request(self, body: dict[str, Any]) -> list[dict[str, Any]]:
        request = urllib.request.Request(
            f"https://api.cloudflare.com/client/v4/accounts/{self.account_id}/d1/database/{self.database_id}/query",
            data=json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "octocore-1of1-monitor/0.1",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds, context=self._ssl_context) as response:
                payload = json.loads(response.read())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            raise RpcError(f"D1 request failed: {error}") from error
        if not payload.get("success"):
            raise RpcError(f"D1 API error: {payload.get('errors')}")
        results = payload.get("result", [])
        if any(not result.get("success", False) for result in results):
            raise RpcError(f"D1 SQL error: {results}")
        return results

    @staticmethod
    def _params(values: Iterable[Any]) -> list[str]:
        return [str(value) for value in values]

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        result = self._request({"sql": sql, "params": self._params(params)})[0]
        return result.get("results", [])

    def batch(self, statements: list[tuple[str, Iterable[Any]]]) -> list[dict[str, Any]]:
        return self._request({"batch": [{"sql": sql, "params": self._params(params)} for sql, params in statements]})

    def migrate(self) -> None:
        self.query(
            """
            CREATE TABLE IF NOT EXISTS blocks (block_number INTEGER PRIMARY KEY, block_hash TEXT NOT NULL, timestamp INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS windows (window_id INTEGER PRIMARY KEY, start_token_id INTEGER NOT NULL, end_token_id INTEGER NOT NULL, one_of_one_token_id INTEGER, one_of_one_minted INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS mints (token_id INTEGER PRIMARY KEY, tx_hash TEXT NOT NULL UNIQUE, block_number INTEGER NOT NULL, block_hash TEXT NOT NULL, timestamp INTEGER NOT NULL, minter TEXT NOT NULL, price_wei TEXT NOT NULL, seed TEXT NOT NULL, work TEXT NOT NULL, target TEXT NOT NULL, nonce TEXT NOT NULL, window_id INTEGER NOT NULL, position INTEGER NOT NULL, is_1of1 INTEGER NOT NULL, rarity_type TEXT, unique_index INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS uniques (token_id INTEGER PRIMARY KEY, window_id INTEGER NOT NULL UNIQUE, type TEXT NOT NULL, unique_index INTEGER NOT NULL, block_number INTEGER NOT NULL, timestamp INTEGER NOT NULL, tx_hash TEXT NOT NULL UNIQUE, minter TEXT NOT NULL, position INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS sync_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS telegram_subscribers (chat_id TEXT PRIMARY KEY, username TEXT, subscribed_at INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_mints_block ON mints(block_number, token_id);
            """
        )

    def insert_mint(self, mint: MintEvent) -> bool:
        window = mint.window
        rarity = unique_type(mint.unique_index)
        statements: list[tuple[str, Iterable[Any]]] = [
            ("INSERT OR IGNORE INTO blocks(block_number, block_hash, timestamp) VALUES (?, ?, ?)", (mint.block_number, mint.block_hash, mint.timestamp)),
            ("INSERT OR IGNORE INTO windows(window_id, start_token_id, end_token_id) VALUES (?, ?, ?)", (window.id, window.start, window.end)),
            ("INSERT OR IGNORE INTO mints(token_id, tx_hash, block_number, block_hash, timestamp, minter, price_wei, seed, work, target, nonce, window_id, position, is_1of1, rarity_type, unique_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (mint.token_id, mint.tx_hash, mint.block_number, mint.block_hash, mint.timestamp, mint.minter.lower(), mint.price_wei, mint.seed, mint.work, mint.target, mint.nonce, window.id, mint.position, int(mint.is_unique), rarity, mint.unique_index)),
        ]
        if mint.is_unique:
            statements.extend([
                ("UPDATE windows SET one_of_one_token_id = ?, one_of_one_minted = 1 WHERE window_id = ?", (mint.token_id, window.id)),
                ("INSERT OR IGNORE INTO uniques(token_id, window_id, type, unique_index, block_number, timestamp, tx_hash, minter, position) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (mint.token_id, window.id, rarity or "UNKNOWN", mint.unique_index, mint.block_number, mint.timestamp, mint.tx_hash, mint.minter.lower(), mint.position)),
            ])
        results = self.batch(statements)
        return bool(results[2].get("meta", {}).get("changes", 0))

    def insert_mints(self, mints: list[MintEvent]) -> list[bool]:
        statements: list[tuple[str, Iterable[Any]]] = []
        mint_indexes: list[int] = []
        for mint in mints:
            window = mint.window
            rarity = unique_type(mint.unique_index)
            statements.extend([
                ("INSERT OR IGNORE INTO blocks(block_number, block_hash, timestamp) VALUES (?, ?, ?)", (mint.block_number, mint.block_hash, mint.timestamp)),
                ("INSERT OR IGNORE INTO windows(window_id, start_token_id, end_token_id) VALUES (?, ?, ?)", (window.id, window.start, window.end)),
            ])
            mint_indexes.append(len(statements))
            statements.append(("INSERT OR IGNORE INTO mints(token_id, tx_hash, block_number, block_hash, timestamp, minter, price_wei, seed, work, target, nonce, window_id, position, is_1of1, rarity_type, unique_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (mint.token_id, mint.tx_hash, mint.block_number, mint.block_hash, mint.timestamp, mint.minter.lower(), mint.price_wei, mint.seed, mint.work, mint.target, mint.nonce, window.id, mint.position, int(mint.is_unique), rarity, mint.unique_index)))
            if mint.is_unique:
                statements.extend([
                    ("UPDATE windows SET one_of_one_token_id = ?, one_of_one_minted = 1 WHERE window_id = ?", (mint.token_id, window.id)),
                    ("INSERT OR IGNORE INTO uniques(token_id, window_id, type, unique_index, block_number, timestamp, tx_hash, minter, position) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (mint.token_id, window.id, rarity or "UNKNOWN", mint.unique_index, mint.block_number, mint.timestamp, mint.tx_hash, mint.minter.lower(), mint.position)),
                ])
        results = self.batch(statements)
        return [bool(results[index].get("meta", {}).get("changes", 0)) for index in mint_indexes]

    def mint_count(self) -> int:
        return int(self.query("SELECT COUNT(*) AS count FROM mints")[0]["count"])

    def _mint_query(self, where: str, params: Iterable[Any] = ()) -> Optional[StoredMint]:
        rows = self.query(f"SELECT token_id, unique_index, window_id, position, block_number, timestamp, tx_hash, minter FROM mints {where}", params)
        return StoredMint(**rows[0]) if rows else None

    def latest_mint(self) -> Optional[StoredMint]:
        return self._mint_query("ORDER BY token_id DESC LIMIT 1")

    def last_unique(self) -> Optional[StoredMint]:
        return self._mint_query("WHERE is_1of1 = 1 ORDER BY token_id DESC LIMIT 1")

    def window_has_unique(self, window_id: int) -> bool:
        rows = self.query("SELECT one_of_one_minted FROM windows WHERE window_id = ?", (window_id,))
        return bool(rows and rows[0]["one_of_one_minted"])

    def unique_for_window(self, window_id: int) -> Optional[StoredMint]:
        return self._mint_query("WHERE window_id = ? AND is_1of1 = 1 LIMIT 1", (window_id,))

    def next_state(self) -> NextMintState:
        latest = self.latest_mint()
        return state_for_next_mint(0, False) if latest is None else state_for_next_mint(latest.token_id, self.window_has_unique(window_for_token(latest.token_id).id))

    def mints_since_last_unique(self) -> Optional[int]:
        latest, unique = self.latest_mint(), self.last_unique()
        return latest.token_id - unique.token_id if latest and unique else None

    def get_sync_block(self) -> Optional[int]:
        rows = self.query("SELECT value FROM sync_state WHERE key = 'last_scanned_block'")
        return int(rows[0]["value"]) if rows else None

    def set_sync_block(self, block_number: int) -> None:
        self.query("INSERT INTO sync_state(key, value) VALUES ('last_scanned_block', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (block_number,))

    def get_telegram_update_offset(self) -> Optional[int]:
        rows = self.query("SELECT value FROM sync_state WHERE key = 'telegram_update_offset'")
        return int(rows[0]["value"]) if rows else None

    def set_telegram_update_offset(self, offset: int) -> None:
        self.query("INSERT INTO sync_state(key, value) VALUES ('telegram_update_offset', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (offset,))

    def unique_rows(self) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM uniques ORDER BY window_id")

    def add_subscriber(self, chat_id: str, username: Optional[str] = None) -> None:
        self.query("INSERT OR IGNORE INTO telegram_subscribers(chat_id, username, subscribed_at) VALUES (?, ?, ?)", (str(chat_id), username, int(time.time())))

    def subscriber_chat_ids(self) -> list[str]:
        return [row["chat_id"] for row in self.query("SELECT chat_id FROM telegram_subscribers ORDER BY subscribed_at")]


class RpcError(RuntimeError):
    pass


def trusted_ssl_context() -> ssl.SSLContext:
    """Use a verified system CA bundle when Python's framework bundle is absent."""
    configured = os.environ.get("SSL_CERT_FILE")
    candidates = [configured] if configured else []
    candidates.extend(("/etc/ssl/cert.pem", "/etc/ssl/certs/ca-certificates.crt"))
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return ssl.create_default_context(cafile=candidate)
    return ssl.create_default_context()


class BlockscoutRpc:
    def __init__(self, api_key: str, timeout_seconds: int = 30) -> None:
        if not api_key.startswith("proapi_"):
            raise ValueError("BLOCKSCOUT_PRO_API_KEY must start with proapi_")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self._request_id = 0
        self._blocks: dict[int, tuple[str, int]] = {}
        self._prices: dict[str, int] = {}
        self._ssl_context = trusted_ssl_context()

    def _post(self, payload: Any) -> Any:
        encoded = json.dumps(payload).encode()
        request = urllib.request.Request(
            f"https://api.blockscout.com/{CHAIN_ID}/json-rpc",
            data=encoded,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "octocore-1of1-monitor/0.1",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds, context=self._ssl_context) as response:
                return json.loads(response.read())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            raise RpcError(f"RPC request failed: {error}") from error

    def call(self, method: str, params: list[Any]) -> Any:
        self._request_id += 1
        body = self._post({"jsonrpc": "2.0", "method": method, "params": params, "id": self._request_id})
        if body.get("error"):
            raise RpcError(f"{method} RPC error: {body['error']}")
        return body["result"]

    def call_batch(self, calls: list[tuple[str, list[Any]]], batch_size: int = 10) -> list[Any]:
        results: list[Any] = []
        for offset in range(0, len(calls), batch_size):
            chunk = calls[offset : offset + batch_size]
            payload = []
            ids = []
            for method, params in chunk:
                self._request_id += 1
                ids.append(self._request_id)
                payload.append({"jsonrpc": "2.0", "method": method, "params": params, "id": self._request_id})
            body = self._post(payload)
            if not isinstance(body, list):
                raise RpcError(f"batch RPC response is not a list: {body}")
            by_id = {item.get("id"): item for item in body}
            for request_id, (method, _) in zip(ids, chunk):
                item = by_id.get(request_id)
                if not item:
                    raise RpcError(f"batch RPC response omitted {method}")
                if item.get("error"):
                    raise RpcError(f"{method} RPC error: {item['error']}")
                results.append(item["result"])
        return results

    def historical_mined_logs(self) -> Iterable[dict[str, Any]]:
        """Yield the indexed Mined events with timestamps, newest-page cursor first."""
        cursor: dict[str, Any] = {"topic": MINED_TOPIC, "items_count": 50}
        while cursor:
            query = urllib.parse.urlencode(cursor)
            request = urllib.request.Request(
                f"https://api.blockscout.com/{CHAIN_ID}/api/v2/addresses/{COLLECTION_ADDRESS}/logs?{query}",
                headers={"Authorization": f"Bearer {self.api_key}", "User-Agent": "octocore-1of1-monitor/0.1", "Accept": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds, context=self._ssl_context) as response:
                    page = json.loads(response.read())
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
                raise RpcError(f"historical logs request failed: {error}") from error
            for log in page.get("items", []):
                yield log
            next_cursor = page.get("next_page_params")
            cursor = ({"topic": MINED_TOPIC, "items_count": 50, **next_cursor} if next_cursor else {})

    def parse_indexed_mined(self, log: dict[str, Any]) -> MintEvent:
        data = log["data"][2:]
        topics = log["topics"]
        if len(topics) < 3 or len(data) != 64 * 6:
            raise RpcError("unexpected indexed Mined event encoding")
        words = ["0x" + data[index : index + 64] for index in range(0, len(data), 64)]
        timestamp = int(datetime.fromisoformat(log["block_timestamp"].replace("Z", "+00:00")).timestamp())
        token_id = int(topics[1], 16)
        return MintEvent(
            token_id=token_id,
            tx_hash=log["transaction_hash"],
            block_number=int(log["block_number"]),
            block_hash=log["block_hash"],
            timestamp=timestamp,
            minter="0x" + topics[2][-40:],
            price_wei=mint_price_wei(token_id),
            seed=words[0],
            work=words[1],
            target=words[3],
            nonce=words[4],
            unique_index=int(words[5], 16),
        )

    def block_number(self) -> int:
        return int(self.call("eth_blockNumber", []), 16)

    def mined_logs(self, from_block: int, to_block: int) -> list[dict[str, Any]]:
        if to_block < from_block:
            return []
        return self.call(
            "eth_getLogs",
            [
                {
                    "address": COLLECTION_ADDRESS,
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                    "topics": [MINED_TOPIC],
                }
            ],
        )

    def block(self, block_number: int) -> tuple[str, int]:
        if block_number not in self._blocks:
            result = self.call("eth_getBlockByNumber", [hex(block_number), False])
            if result is None:
                raise RpcError(f"block {block_number} was not found")
            self._blocks[block_number] = (result["hash"], int(result["timestamp"], 16))
        return self._blocks[block_number]

    def transaction_value(self, tx_hash: str) -> int:
        if tx_hash not in self._prices:
            result = self.call("eth_getTransactionByHash", [tx_hash])
            if result is None:
                raise RpcError(f"transaction {tx_hash} was not found")
            self._prices[tx_hash] = int(result["value"], 16)
        return self._prices[tx_hash]

    def prefetch_mined_logs(self, logs: Iterable[dict[str, Any]]) -> None:
        logs = list(logs)
        missing_blocks = sorted(
            {int(log["blockNumber"], 16) for log in logs if int(log["blockNumber"], 16) not in self._blocks}
        )
        block_results = [self.call("eth_getBlockByNumber", [hex(block_number), False]) for block_number in missing_blocks]
        for block_number, result in zip(missing_blocks, block_results):
            if result is None:
                raise RpcError(f"block {block_number} was not found")
            self._blocks[block_number] = (result["hash"], int(result["timestamp"], 16))

        missing_txs = sorted({log["transactionHash"] for log in logs if log["transactionHash"] not in self._prices})
        transaction_results = [self.call("eth_getTransactionByHash", [tx_hash]) for tx_hash in missing_txs]
        for tx_hash, result in zip(missing_txs, transaction_results):
            if result is None:
                raise RpcError(f"transaction {tx_hash} was not found")
            self._prices[tx_hash] = int(result["value"], 16)

    def parse_mined(self, log: dict[str, Any]) -> MintEvent:
        topics = log["topics"]
        data = log["data"][2:]
        if len(topics) < 3 or len(data) != 64 * 6:
            raise RpcError("unexpected Mined event encoding")
        block_number = int(log["blockNumber"], 16)
        block_hash, timestamp = self.block(block_number)
        words = ["0x" + data[index : index + 64] for index in range(0, len(data), 64)]
        return MintEvent(
            token_id=int(topics[1], 16),
            tx_hash=log["transactionHash"],
            block_number=block_number,
            block_hash=block_hash,
            timestamp=timestamp,
            minter="0x" + topics[2][-40:],
            price_wei=self.transaction_value(log["transactionHash"]),
            seed=words[0],
            work=words[1],
            target=words[3],
            nonce=words[4],
            unique_index=int(words[5], 16),
        )


class PublicInkRpc:
    """Failover HTTP client used only for live confirmations and gap resync."""

    def __init__(self, endpoints: tuple[str, ...] = PUBLIC_HTTPS_RPCS, timeout_seconds: int = 20) -> None:
        self.endpoints = endpoints
        self.timeout_seconds = timeout_seconds
        self._request_id = 0
        self._blocks: dict[int, tuple[str, int]] = {}
        self._prices: dict[str, int] = {}
        self._ssl_context = trusted_ssl_context()

    def call(self, method: str, params: list[Any]) -> Any:
        self._request_id += 1
        payload = json.dumps({"jsonrpc": "2.0", "method": method, "params": params, "id": self._request_id}).encode()
        last_error: Optional[Exception] = None
        for endpoint in self.endpoints:
            request = urllib.request.Request(
                endpoint,
                data=payload,
                headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "octocore-1of1-monitor/0.1"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds, context=self._ssl_context) as response:
                    body = json.loads(response.read())
                if body.get("error"):
                    raise RpcError(f"{method} RPC error from {endpoint}: {body['error']}")
                return body["result"]
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, RpcError) as error:
                last_error = error
        raise RpcError(f"all public Ink RPC endpoints failed for {method}: {last_error}")

    block_number = BlockscoutRpc.block_number
    mined_logs = BlockscoutRpc.mined_logs
    block = BlockscoutRpc.block
    transaction_value = BlockscoutRpc.transaction_value
    prefetch_mined_logs = BlockscoutRpc.prefetch_mined_logs
    parse_mined = BlockscoutRpc.parse_mined


class InkWebSocket:
    def __init__(self, endpoints: tuple[str, ...] = PUBLIC_WS_RPCS, timeout_seconds: int = 45) -> None:
        self.endpoints = endpoints
        self.timeout_seconds = timeout_seconds
        self._ssl_context = trusted_ssl_context()

    def events(self) -> Iterable[tuple[str, Any]]:
        last_error: Optional[Exception] = None
        for endpoint in self.endpoints:
            ws = None
            try:
                ssl_options = {"cert_reqs": ssl.CERT_REQUIRED}
                if self._ssl_context.get_ca_certs():
                    ssl_options["ca_certs"] = os.environ.get("SSL_CERT_FILE", "/etc/ssl/cert.pem")
                ws = websocket.create_connection(endpoint, timeout=self.timeout_seconds, sslopt=ssl_options)
                ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
                ws.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe", "params": ["logs", {"address": COLLECTION_ADDRESS, "topics": [MINED_TOPIC]}]}))
                yield ("connected", endpoint)
                while True:
                    message = json.loads(ws.recv())
                    if message.get("method") != "eth_subscription":
                        continue
                    result = message["params"]["result"]
                    if "number" in result:
                        yield ("head", int(result["number"], 16))
                    elif not result.get("removed", False):
                        yield ("log", result)
            except (OSError, websocket.WebSocketException, json.JSONDecodeError) as error:
                last_error = error
            finally:
                if ws:
                    ws.close()
        raise RpcError(f"all public Ink WebSocket endpoints failed: {last_error}")


def percentage(probability: Fraction) -> str:
    return f"{float(probability * 100):.4f}%"


def format_mint_alert(mint: MintEvent, store: Any) -> str:
    state = store.next_state()
    last_unique = store.last_unique()
    rarity = unique_type(mint.unique_index)
    current_unique = store.unique_for_window(state.window.id)
    unique_status = (
        f"MINTED — {unique_type(current_unique.unique_index)}" if current_unique else "NOT MINTED"
    )
    address_url = f"https://debank.com/profile/{mint.minter}"
    tx_url = f"https://explorer.inkonchain.com/tx/{mint.tx_hash}"
    lines = [
        "🐙 <b>OCTOCORE MINT</b>",
        "",
        f"Token: #{mint.token_id}",
        f'Minter: <a href="{escape(address_url, quote=True)}">{escape(mint.minter)}</a>',
        f"Price: {mint.price_wei / 10**18:.6f} ETH",
        f'Tx: <a href="{escape(tx_url, quote=True)}">{escape(mint.tx_hash)}</a>',
        "",
        f"Next window mint: #{state.next_token_id}",
        f"Window: #{state.window.id} ({state.window.start}–{state.window.end})",
        f"Position after mint: {state.position} / {state.window.size}",
        f"Remaining: {state.remaining}",
        f"Current 1/1: {unique_status}",
        "",
        f"Chance that the next valid mint is a 1/1: {float(state.probability_next * 100):.2f}%",
        "This chance increases as the window fills if its 1/1 has not appeared yet; after a 1/1 is minted in the window, it becomes 0%.",
    ]
    if last_unique:
        lines.extend(
            [
                "",
                f"Last 1/1: #{last_unique.token_id} ({unique_type(last_unique.unique_index)})",
                f"Mints since last 1/1: {store.mints_since_last_unique()}",
            ]
        )
    if mint.is_unique:
        lines[0] = "🔥 <b>NEW OCTOCORE 1/1</b>"
        lines.insert(2, f"Type: {rarity}")
    lines.extend(["", "follow me: https://x.com/IntelPocik", "and have a good luck mate 🫡"])
    return "\n".join(lines)


def should_send_mint_alert(store: Any) -> bool:
    return store.next_state().probability_next > 0


def format_start_message(store: Any) -> str:
    state = store.next_state()
    current_unique = store.unique_for_window(state.window.id)
    lines = [
        "🐙 <b>OCTOCORE 1/1 MONITOR</b>",
        "",
        f"Current window: #{state.window.id} ({state.window.start}–{state.window.end})",
        f"Next mint: #{state.next_token_id}",
        f"Remaining NFTs: {state.remaining}",
        "",
    ]
    if current_unique:
        chance_at_win = candidate_probability(state.window.end - current_unique.token_id + 1)
        lines.extend([
            f"Current 1/1: MINTED — {unique_type(current_unique.unique_index)}",
            f"Winning mint: #{current_unique.token_id} ({current_unique.position}/{state.window.size})",
            f"Chance at the winning mint: {float(chance_at_win * 100):.2f}%",
            "Current chance to mint a 1/1: 0.00%",
            "",
            "The 1/1 for this window has already been claimed. I will notify you when the next window begins and the chance is above 0%. Stay tuned.",
        ])
    else:
        lines.extend([
            "Current 1/1: NOT MINTED",
            f"Current chance to mint a 1/1: {float(state.probability_next * 100):.2f}%",
            "",
            "The 1/1 is still available in this window. You will receive alerts while the chance remains above 0%.",
        ])
    return "\n".join(lines)


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id

    def _call(self, method: str, payload: dict[str, Any], timeout_seconds: int = 20) -> dict[str, Any]:
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self.token}/{method}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds, context=trusted_ssl_context()) as response:
                result = json.loads(response.read())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            raise RpcError(f"Telegram {method} failed: {error}") from error
        if not result.get("ok"):
            raise RpcError(f"Telegram {method} failed: {result}")
        return result

    def send_to(self, chat_id: str, text: str) -> None:
        self._call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})

    def send(self, text: str) -> None:
        self.send_to(self.chat_id, text)

    def get_updates(self, offset: Optional[int]) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"timeout": 25, "allowed_updates": ["message"]}
        if offset is not None:
            payload["offset"] = offset
        # Telegram long polling deliberately waits for `timeout`; the HTTP timeout
        # must be longer or an idle bot produces a false error every poll.
        return self._call("getUpdates", payload, timeout_seconds=35)["result"]


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def require_rpc_from_env() -> BlockscoutRpc:
    api_key = os.environ.get("BLOCKSCOUT_PRO_API_KEY", "")
    if not api_key:
        raise SystemExit("BLOCKSCOUT_PRO_API_KEY is required. Put it in a gitignored .env file.")
    return BlockscoutRpc(api_key)


def require_public_ink_rpc() -> PublicInkRpc:
    return PublicInkRpc()


def ingest_range(store: Any, rpc: BlockscoutRpc, from_block: int, to_block: int) -> list[MintEvent]:
    events: list[MintEvent] = []
    logs = sorted(rpc.mined_logs(from_block, to_block), key=lambda item: (int(item["blockNumber"], 16), int(item["logIndex"], 16)))
    rpc.prefetch_mined_logs(logs)
    for log in logs:
        mint = rpc.parse_mined(log)
        if store.insert_mint(mint):
            events.append(mint)
    store.set_sync_block(to_block)
    return events


def ingest_logs(store: Any, rpc: Any, logs: Iterable[dict[str, Any]]) -> list[MintEvent]:
    logs = sorted(logs, key=lambda item: (int(item["blockNumber"], 16), int(item["logIndex"], 16)))
    if not logs:
        return []
    rpc.prefetch_mined_logs(logs)
    events: list[MintEvent] = []
    for log in logs:
        mint = rpc.parse_mined(log)
        if store.insert_mint(mint):
            events.append(mint)
    store.set_sync_block(max(event.block_number for event in events) if events else max(int(log["blockNumber"], 16) for log in logs))
    return events


def notify_mints(mints: Iterable[MintEvent], store: Any, notifier: Optional[TelegramNotifier]) -> None:
    for mint in mints:
        logging.info(json.dumps({"event": "mint", "token_id": mint.token_id, "unique": mint.is_unique, "tx": mint.tx_hash}))
        if notifier and should_send_mint_alert(store):
            recipients = store.subscriber_chat_ids() or [notifier.chat_id]
            for chat_id in recipients:
                notifier.send_to(chat_id, format_mint_alert(mint, store))
        elif notifier:
            logging.debug(json.dumps({"event": "mint_alert_suppressed", "token_id": mint.token_id, "reason": "current_window_1of1_already_minted"}))


def telegram_update_loop(store: Any, notifier: TelegramNotifier) -> None:
    offset = store.get_telegram_update_offset()
    while True:
        try:
            for update in notifier.get_updates(offset):
                offset = int(update["update_id"]) + 1
                store.set_telegram_update_offset(offset)
                message = update.get("message")
                if not message or message.get("text", "").split()[0:1] != ["/start"]:
                    continue
                chat = message["chat"]
                username = message.get("from", {}).get("username")
                chat_id = str(chat["id"])
                store.add_subscriber(chat_id, username)
                notifier.send_to(chat_id, format_start_message(store))
                logging.info(json.dumps({"event": "telegram_subscriber_started", "chat_id": chat_id, "username": username}))
        except (RpcError, urllib.error.URLError) as error:
            logging.error(json.dumps({"event": "telegram_update_error", "error": str(error)}))
            time.sleep(5)


def backfill(store: Any, rpc: BlockscoutRpc, start_block: int, chunk_size: int = 2_000) -> int:
    if isinstance(store, D1Store):
        added = 0
        processed = 0
        batch: list[MintEvent] = []
        for log in rpc.historical_mined_logs():
            batch.append(rpc.parse_indexed_mined(log))
            if len(batch) < 20:
                continue
            added += sum(store.insert_mints(batch))
            processed += len(batch)
            if processed % 200 == 0:
                logging.info(json.dumps({"event": "backfill_progress", "processed": processed, "added": added}))
            batch = []
        if batch:
            added += sum(store.insert_mints(batch))
            processed += len(batch)
            logging.info(json.dumps({"event": "backfill_complete", "processed": processed, "added": added}))
        head = rpc.block_number()
        store.set_sync_block(head)
        return added
    head = rpc.block_number()
    added = 0
    for from_block in range(start_block, head + 1, chunk_size):
        to_block = min(from_block + chunk_size - 1, head)
        added += len(ingest_range(store, rpc, from_block, to_block))
        logging.info(json.dumps({"event": "backfill_chunk", "from": from_block, "to": to_block, "added": added}))
    return added


def monitor(store: Any, rpc: BlockscoutRpc, notifier: Optional[TelegramNotifier], poll_seconds: float, confirmations: int) -> None:
    logging.info(json.dumps({"event": "monitor_started", "poll_seconds": poll_seconds, "confirmations": confirmations}))
    while True:
        try:
            head = rpc.block_number()
            safe_head = head - confirmations
            cursor = store.get_sync_block()
            if cursor is None:
                store.set_sync_block(safe_head)
            elif safe_head > cursor:
                notify_mints(ingest_range(store, rpc, cursor + 1, safe_head), store, notifier)
            time.sleep(poll_seconds)
        except KeyboardInterrupt:
            logging.info(json.dumps({"event": "monitor_stopped"}))
            return
        except (RpcError, urllib.error.URLError) as error:
            logging.error(json.dumps({"event": "monitor_error", "error": str(error)}))
            time.sleep(max(poll_seconds, 5.0))


def monitor_websocket(store: Any, rpc: PublicInkRpc, notifier: Optional[TelegramNotifier], confirmations: int) -> None:
    retry_seconds = 2.0
    while True:
        pending: dict[int, list[dict[str, Any]]] = {}
        try:
            head = rpc.block_number()
            cursor = store.get_sync_block()
            safe_head = head - confirmations
            if cursor is None:
                store.set_sync_block(safe_head)
            elif safe_head > cursor:
                notify_mints(ingest_range(store, rpc, cursor + 1, safe_head), store, notifier)
            for kind, payload in InkWebSocket().events():
                if kind == "connected":
                    retry_seconds = 2.0
                    logging.info(json.dumps({"event": "websocket_connected", "endpoint": payload}))
                elif kind == "log":
                    pending.setdefault(int(payload["blockNumber"], 16), []).append(payload)
                elif kind == "head":
                    ready_blocks = [block for block in pending if block <= payload - confirmations]
                    for block in sorted(ready_blocks):
                        notify_mints(ingest_logs(store, rpc, pending.pop(block)), store, notifier)
        except KeyboardInterrupt:
            logging.info(json.dumps({"event": "monitor_stopped"}))
            return
        except (RpcError, websocket.WebSocketException, OSError) as error:
            logging.error(json.dumps({"event": "websocket_error", "error": str(error), "retry_seconds": retry_seconds}))
            time.sleep(retry_seconds)
            retry_seconds = min(retry_seconds * 2, 60.0)


def format_analysis(store: Any) -> str:
    uniques = store.unique_rows()
    lines = [f"Total 1/1 found: {len(uniques)}", "", "Window | Start | End | 1/1 token | Position | Type | Time"]
    previous_token: Optional[int] = None
    intervals: list[int] = []
    for row in uniques:
        if previous_token is not None:
            intervals.append(row["token_id"] - previous_token)
        previous_token = row["token_id"]
        lines.append(
            f"{row['window_id']:>6} | {window_for_token(row['token_id']).start:>5} | "
            f"{window_for_token(row['token_id']).end:>5} | {row['token_id']:>10} | "
            f"{row['position']:>3}/{window_for_token(row['token_id']).size} | {row['type']} | {row['timestamp']}"
        )
    if intervals:
        lines.extend(["", f"Average interval: {sum(intervals) / len(intervals):.2f} NFT", f"Min interval: {min(intervals)}", f"Max interval: {max(intervals)}"])
    return "\n".join(lines)
