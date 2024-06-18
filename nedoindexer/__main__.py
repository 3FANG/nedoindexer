import asyncio
import copy
import datetime
import logging
from time import time, sleep
from itertools import chain
from typing import Optional, Union, Literal, Callable
import os

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError
from dotenv import load_dotenv
from pytoniq import BlockIdExt

from nedoindexer.logger import AsyncFileHandler
from nedoindexer.blockchain import BlockchainProcessing
from nedoindexer.db import DatabaseHandler, Jetton, JettonWallet, Wallet
from nedoindexer.encrypt import convert_raw_to_user_friendly
from nedoindexer.request import IndexerRequests
from nedoindexer.proxy import ProxyHandler


logger = logging.getLogger('nedoindexer')
logger.setLevel(logging.DEBUG)

fh = AsyncFileHandler()
fh.setLevel(logging.DEBUG)

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(asctime)s] #%(levelname)s - %(name)s:%(lineno)d - %(message)s')

fh.setFormatter(formatter)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)


load_dotenv()


DB_URL = os.getenv('POSTGRESQL_URL')


def nanocoin_conversion(number_str: str) -> float:
    """Преобразует наномонеты в обычные."""
    # Определяем позицию для десятичной точки
    point_position = len(number_str) - 9
    formatted_str = number_str[:point_position] + '.' + number_str[point_position:]
    number = float(formatted_str)
    return number


def convert_jettons_wallets_from_response(address: str, response: dict) -> Optional[list[JettonWallet]]:
    """
    Преобразует ответ от сервера в список объектов JettonWallet.
    """
    jettons = response['jetton_wallets']
    if jettons:
        last_update = datetime.datetime.now().replace(microsecond=0)

        jettons_wallets_list = []
        for jetton in jettons:
            jetton_wallet = JettonWallet(
                    address,
                    jetton['jetton'].lower(),
                    jetton['address'].lower(),
                    *convert_raw_to_user_friendly(jetton['address'].lower()),
                    nanocoin_conversion(jetton['balance']),
                    last_update
                )
            jettons_wallets_list.append(
                jetton_wallet
            )
        return jettons_wallets_list
    else: 
        return None

def convert_wallet_from_response(address: str, response: dict) -> Wallet:
    """
    Преобразует ответ от сервера в объект Wallet.
    """
    wallet = Wallet(
        address,
        *convert_raw_to_user_friendly(address),
        ''.join(response['wallet_type'].split(' ')[1:]),
        nanocoin_conversion(response['balance']),
        datetime.datetime.now().replace(microsecond=0)
    )
    return wallet


async def fetch_and_process(
        requests_handler: 'IndexerRequests',
        session: aiohttp.ClientSession,
        address: str,
        proxy: 'ProxyHandler.Proxy',
        url: str,
        converter: Callable[[dict], Optional[Union[list[JettonWallet], Wallet]]],
        response_key: Literal['jetton_wallets', 'wallet_type']
    ) -> Optional[Union[list[JettonWallet], Wallet]]:

    """
    Общая функция для обработки запросов и проверок.
    """
    response = await requests_handler.send_request(
        url,
        session,
        address,
        proxy
    )

    if response and response.get(response_key):
        return converter(address, response)
    else:
        return None


async def process_transactions(
        blockchain_handler: BlockchainProcessing,
        block: BlockIdExt,
    ) -> list[str]:
    """
    Обработать транзакции блока.
    
    Помещает полученные адреса в очередь.
    """
    transactions = await blockchain_handler.get_block_transactions(block)
    addresses = await blockchain_handler.get_transaction_addresses(transactions)

    return addresses


async def requests_rate_limiter(
        requests_handler: IndexerRequests,
        session: aiohttp.ClientSession,
        addresses_queue: asyncio.Queue,
        proxy: 'ProxyHandler.Proxy',
        wait_for: float,
        url: str,
        converter: Callable[[dict], Optional[Union[list[JettonWallet], Wallet]]],
        response_key: Literal['jetton_wallets', 'wallet_type']
    ) -> list[Union[list[JettonWallet], Wallet]]:
    """
    Контролирует отправку запроса с определенной частотой.

    У индексатора есть определенные лимиты на количество запросов.
    """
    tasks = []
    while True:
        if addresses_queue.empty():
            break
        address = await addresses_queue.get()

        tasks.append(asyncio.create_task(fetch_and_process(
            requests_handler,
            session,
            address,
            proxy,
            url,
            converter,
            response_key
        )))
        await asyncio.sleep(wait_for)

        addresses_queue.task_done()
    
    return [x for x in await asyncio.gather(*tasks) if x is not None]


async def distribution_requests_between_proxies(
        requests_handler: IndexerRequests,
        proxy_handler: ProxyHandler,
        addresses_queue: asyncio.Queue,
        url: str,
        converter: Callable[[dict], Optional[Union[list[JettonWallet], Wallet]]],
        response_key: Literal['jetton_wallets', 'wallet_type']
    ) -> list[Union[list[JettonWallet], Wallet]]:
    """
    Для каждого прокси создает отдельную задачу для отправки запросов.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for proxy in proxy_handler.get_proxies():
            tasks.append(asyncio.create_task(
                requests_rate_limiter(
                    requests_handler,
                    session,
                    addresses_queue,
                    proxy,
                    requests_handler.request_per,
                    url,
                    converter,
                    response_key
                )
            ))

        await addresses_queue.join()
        return [obj for sublist in await asyncio.gather(*tasks) for obj in sublist]
    

async def fetch_wallets_info(addresses: list[str], requests_handler: IndexerRequests, proxy_handler: ProxyHandler) -> list[Wallet]:
    """
    Собрать информацию о кошельках.
    """
    addresses_queue = asyncio.Queue()
    for address in addresses:
        addresses_queue.put_nowait(address)
    
    return await distribution_requests_between_proxies(
        requests_handler,
        proxy_handler,
        addresses_queue,
        requests_handler.get_wallet_info_url,
        convert_wallet_from_response,
        'wallet_type'
    )


async def fetch_jetton_wallets(wallets: list[Wallet], requests_handler: IndexerRequests, proxy_handler: ProxyHandler, available_jettons: set[str]) -> tuple[list[Jetton], list[JettonWallet]]:
    """
    Собрать информацию о кошельках жетонов.

    Также возвращает список жетонов, которых еще нет в БД.
    """
    addresses_queue = asyncio.Queue()
    for wallet in wallets:
        addresses_queue.put_nowait(wallet.raw_address)

    jettons_wallets_list = await distribution_requests_between_proxies(
        requests_handler,
        proxy_handler,
        addresses_queue,
        requests_handler.get_jetton_wallets_url,
        convert_jettons_wallets_from_response,
        'jetton_wallets',
    )

    jetton_wallets: list[JettonWallet] = []
    new_jettons: list[Jetton] = []
    for sublist in jettons_wallets_list:
        for jetton_wallet in sublist:
            if jetton_wallet.jetton_master not in available_jettons:
                available_jettons.add(jetton_wallet.jetton_master)
                new_jettons.append(Jetton(jetton_wallet.jetton_master, *convert_raw_to_user_friendly(jetton_wallet.jetton_master)))
            jetton_wallets.append(jetton_wallet)

    return new_jettons, jetton_wallets


def check_responses_condition(requests_handler: IndexerRequests, count_addresses: int) -> bool:
    """
    Проверяет состояние ответов от сервера индексатора.

    Если сервер начинает накладывать ограничения, либо вообще игнорирует запросы,
    то возвращаем True.
    """
    response_count = sum(sum(url.values()) for url in requests_handler._responses_condition.values())
    logger.info(f"[~] Всего получено ответов {response_count}: {requests_handler.condition}")

    if requests_handler.condition.get(429, 0) > response_count * 0.01:
        requests_handler.request_per += 0.1
        logger.info(f"[%] Изменение частоты HTTP запросов: стало {requests_handler.request_per}")

    waiting_errors = requests_handler.processed_wallets_count - response_count
    if waiting_errors > requests_handler.processed_wallets_count * 0.005:
        requests_handler.timeout += 2
        logger.info(f"[%] Изменения длительности таймаута: стало {requests_handler.timeout}")

    del requests_handler.condition
    del requests_handler.processed_wallets_count

    if waiting_errors > 10:
        return True
    else:
        return False


async def process_blockchain(
        blockchain_handler: BlockchainProcessing,
        requests_handler: IndexerRequests,
        db_handler: DatabaseHandler,
        proxy_handler: ProxyHandler,
        available_jettons: set[str]
    ):
    """
    Обработать последние сгенерированные блоки.
    """
    async for latest_blocks in blockchain_handler.get_last_blocks():
        start_time = time()
        transaction_tasks = [process_transactions(blockchain_handler, block) for block in latest_blocks]
        addresses = list(chain(*await asyncio.gather(*transaction_tasks)))

        if not addresses:
            continue

        logger.info(f"[+] {len(addresses)} адресов получено.")

        wallets = await fetch_wallets_info(addresses, requests_handler, proxy_handler)
        await db_handler.save_wallets(wallets)

        new_jettons, jetton_wallets = await fetch_jetton_wallets(wallets, requests_handler, proxy_handler, available_jettons)
        await db_handler.save_jettons(new_jettons)
        await db_handler.save_jettons_wallets(jetton_wallets)

        logger.info(f"[~] Всего обработано {len(addresses)} адресов и их жетонов за {time() - start_time} сек.")

        if check_responses_condition(requests_handler, len(addresses)):
            logger.warning(f"[!] Критическое состояние обращений к серверу, перезапуск обработки.")
            break


async def main():
    blockchain_handler = BlockchainProcessing()

    await blockchain_handler.start_up()

    db_handler = DatabaseHandler(DB_URL)
    await db_handler.connect()

    proxy_handler = ProxyHandler()
    proxy_handler.set_proxies()

    requests_handler = IndexerRequests()

    available_jettons = {*await db_handler.get_jettons_addresses()}
    
    blockchain_processing_task = asyncio.create_task(
        process_blockchain(
            blockchain_handler,
            requests_handler,
            db_handler,
            proxy_handler,
            available_jettons,
        )
    )

    await asyncio.gather(blockchain_processing_task)

    await blockchain_handler.shutdown()

    await db_handler.close()


if __name__ == '__main__':
    while True:
        asyncio.run(main())
        sleep(15)
