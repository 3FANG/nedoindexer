import logging
from typing import AsyncIterator

from pytoniq import LiteBalancer, Transaction, BlockIdExt
from pytoniq.liteclient.client import LiteServerError


logger = logging.getLogger('nedoindexer.blockchain')


class BlockchainProcessing:
    """Класс для взаимодействия с блокчейном."""

    def __init__(self, trust_level: int=2) -> None:
        self.client = LiteBalancer.from_mainnet_config(trust_level)

    async def start_up(self):
        """Запустить клиент для работы с блокчейном."""
        logger.info("[^] Запуск клиента")
        try:
            await self.client.start_up()
        except IndexError as ex: # block that knows at least 2/3 liteservers (pytoniq/liteclient/balancer.py, line 207, in _find_consensus_bloc)
            logger.error(f"Ошибка 'block that knows at least 2/3 liteservers': {ex}")
            await self.client.start_up()

    async def shutdown(self):
        """Закрыть клиент."""
        await self.client.close_all()
        logger.info("[^] Клиент закрыт")


    async def get_last_blocks(self) -> AsyncIterator[list[BlockIdExt]]:
        """
        Получать последние сгенерированные блоки.
        
        Делает запрос на получение последних сгенерированных блоков, проверяя,
        были ли уже получены эти блоки, если нет, то возвращает список с новыми блоками,
        в противном случае делает повторный запрос.
        """
        processed_blocks = set()

        while True:
            latest_blocks = await self.client.get_all_shards_info()
            allowed_blocks = []

            for block in latest_blocks:
                if block.seqno in processed_blocks:
                    continue
                else:
                    processed_blocks.add(block.seqno)
                    allowed_blocks.append(block)
                    logger.info(f"Получен блок [wc={block.workchain}, shard={block.shard}, seqno={block.seqno}]")

            yield allowed_blocks

    async def get_block_transactions(self, block: BlockIdExt) -> list[Transaction]:
        """Получить транзакции блока."""
        while True:
            try:
                transactions = await self.client.raw_get_block_transactions_ext(block)
                logger.info(f"В блоке [wc={block.workchain}, shard={block.shard}, seqno={block.seqno}] {len(transactions)} транзакций.")
            except LiteServerError:
                continue
            else:
                return transactions
            
    async def get_transaction_addresses(self, transactions: list[Transaction]) -> list[str]:
        """
        Получить адреса (отправитель и получатель) транзакций.

        Обрабатывает только внутреннние транзакции.
        """
        addresses = []

        for tr in transactions:
            if not tr.in_msg.is_internal:
                continue

            message = tr.in_msg.info
            src = ':'.join(str(value) for value in message.src.to_tl_account_id().values())
            dest = ':'.join(str(value) for value in message.dest.to_tl_account_id().values())

            addresses.extend([src, dest])
        
        return addresses