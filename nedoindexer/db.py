import logging
from typing import NamedTuple
from datetime import datetime

import asyncpg


logger = logging.getLogger('nedoindexer.db')


class Wallet(NamedTuple):
    """Представление основного кошелька."""
    raw_address: str
    bounceable_jetton_wallet: str
    nonbounceable_jetton_wallet: str
    wallet_type: str
    balance: int
    last_update: datetime


class Jetton(NamedTuple):
    """Представление жетона."""
    raw_address: str
    bounceable_jetton_wallet: str
    nonbounceable_jetton_wallet: str


class JettonWallet(NamedTuple):
    """Представление кошелька жетона."""
    owner_address: str
    jetton_master: str
    raw_jetton_wallet: str
    bounceable_jetton_wallet: str
    nonbounceable_jetton_wallet: str
    balance: int
    last_update: datetime


class DatabaseHandler:
    """Класс для взаимодействия с БД.
    
    Account (
        raw_address VARCHAR(67) PRIMARY KEY,
        is_bounceable_address VARCHAR(50),
        non_bounceable_address VARCHAR(50),
        wallet_type VARCHAR(5),
        balance NUMERIC,
        last_update TIMESTAMP
    )

    Jetton (
        raw_address VARCHAR(67) PRIMARY KEY,
        is_bounceable_address VARCHAR(50),
        non_bounceable_address VARCHAR(50)
    )

    AccountJettons (
        owner_wallet VARCHAR(67) REFERENCES Account(raw_address),
        jetton_master VARCHAR(67) REFERENCES Jetton(raw_address),
        raw_address VARCHAR(67),
        is_bounceable_address VARCHAR(50),
        non_bounceable_address VARCHAR(50),
        balance NUMERIC,
        last_update TIMESTAMP,
        PRIMARY KEY (owner_wallet, jetton_master)
    )
    """

    def __init__(self, db_url: str) -> None:
        self.db_url = db_url
        self.pool = None
        self.insert_account_expression = "INSERT INTO Account VALUES ($1, $2, $3, $4, $5, $6) \
            ON CONFLICT (raw_address) DO UPDATE SET balance = EXCLUDED.balance, last_update = EXCLUDED.last_update"
        self.insert_jetton_expression = "INSERT INTO Jetton VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
        self.insert_accountjetton_expression = "INSERT INTO AccountJettons VALUES ($1, $2, $3, $4, $5, $6, $7) \
            ON CONFLICT (owner_wallet, jetton_master) DO UPDATE SET balance = EXCLUDED.balance, last_update = EXCLUDED.last_update"
        self.select_jettons_expresssion = "SELECT raw_address FROM Jetton"

    async def connect(self):
        """Инициализировать пул соединений."""
        self.pool = await asyncpg.create_pool(self.db_url)

    async def close(self):
        """Закрыть пул соединений."""
        await self.pool.close()

    async def save_wallets(self, wallets: list[Wallet]):
        """Сохранить адреса в БД."""
        async with self.pool.acquire() as connection:
            await connection.executemany(self.insert_account_expression, [tuple(wallet) for wallet in wallets])
        logger.info(f"[+] {len(wallets)} кошельков вставлены в БД.")

    async def save_jettons(self, jettons: list[Jetton]):
        """Сохранить жетон в БД."""
        async with self.pool.acquire() as connection:
            await connection.executemany(self.insert_jetton_expression, [tuple(jetton) for jetton in jettons])
        logger.info(f"[+] {len(jettons)} жетонов записан в БД.")

    async def save_jettons_wallets(self, jettons_wallets: list[JettonWallet]):
        """Сохранить жетоны кошелька."""        
        async with self.pool.acquire() as connection:
            await connection.executemany(
                self.insert_accountjetton_expression,
                [tuple(jetton_wallet) for jetton_wallet in jettons_wallets]
            )
        logger.info(f"[+] {len(jettons_wallets)} кошельков жетонов записаны в БД.")

    async def get_jettons_addresses(self) -> list[str]:
        """Получить список raw-адресов жетонов."""
        async with self.pool.acquire() as connection:
            records = await connection.fetch(self.select_jettons_expresssion)
        logger.info(f"[+] Из БД получены {len(records)} жетонов.")
        return [record['raw_address'] for record in records]