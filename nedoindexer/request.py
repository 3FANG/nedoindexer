import asyncio
import inspect
import logging

import aiohttp

from nedoindexer.proxy import ProxyHandler


logger = logging.getLogger(f"nedoindexer.requests")


class IndexerRequests:
    """Класс для сетевых запросов в индексатор."""

    def __init__(self, timeout: float=10, request_per: float=0.2) -> None:
        self.get_wallet_info_url = "https://toncenter.com/api/v3/wallet?address"
        self.get_jetton_wallets_url = "https://toncenter.com/api/v3/jetton/wallets?owner_address"
        self._timeout = timeout
        self._request_per = request_per
        self._responses_condition = {
            self.get_wallet_info_url: {},
            self.get_jetton_wallets_url: {}
        }
        self._processed_wallets_count = 0

    @staticmethod
    def timeout_handling(coroutine):
        """
        Декоратор для обработки ошибки истечения таймаута.
        """
        async def wrapper(*args, **kwargs):
            try:
                result = await coroutine(*args, **kwargs)
            except asyncio.exceptions.TimeoutError:
                singnature = inspect.signature(coroutine)
                bound_args = singnature.bind(*args, **kwargs)
                bound_args.apply_defaults()

                proxy: 'ProxyHandler.Proxy' = bound_args.arguments.get('proxy', None)

                logger.error(f"[-] Истекло время ожидания прокси {proxy.address}")
            else:
                return result

        return wrapper

    @timeout_handling
    async def send_request(self, url: str, session: aiohttp.ClientSession, address: str, proxy: 'ProxyHandler.Proxy') -> dict|None:
        """
        Послать HTTP запрос в индекастор.
        """
        request_url = f"{url}={address}&api_key={proxy.key}"
        headers = {'User-Agent': proxy.user_agent}

        self.processed_wallets_count += 1
        async with session.get(request_url, proxy=proxy.address, timeout=self.timeout, headers=headers) as response:
            self._update_response_condition(url, response.status)
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                logger.info(f"[-] 429 Too Many Requests {proxy.address}")
            else:
                return None

    def _update_response_condition(self, url: str, status: int) -> None:
        if url not in self._responses_condition:
            self._responses_condition[url] = {}
        self._responses_condition[url][status] = self._responses_condition[url].get(status, 0) + 1

        
    @property
    def timeout(self):
        return self._timeout
    
    @timeout.setter
    def timeout(self, value):
        self._timeout = value

    @property
    def request_per(self):
        return self._request_per
    
    @request_per.setter
    def request_per(self, value):
        self._request_per = value

    @property
    def condition(self):
        return self._responses_condition
    
    @condition.deleter
    def condition(self):
        self._responses_condition = {}

    @property
    def processed_wallets_count(self):
        return self._processed_wallets_count
    
    @processed_wallets_count.setter
    def processed_wallets_count(self, value):
        self._processed_wallets_count = value

    @processed_wallets_count.deleter
    def processed_wallets_count(self):
        self._processed_wallets_count = 0