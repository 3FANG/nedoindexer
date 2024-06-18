from typing import NamedTuple
from fake_useragent import UserAgent


class ProxyHandler:
    """Класс для работы с прокси, юзер-агентами и ключами."""

    class Proxy(NamedTuple):
        """Представление прокси."""
        address: str
        key: str
        user_agent: str
    
    def __init__(self) -> None:
        self.proxies: list['ProxyHandler.Proxy'] = []
        self.ua = UserAgent()

    def set_proxies(self, file='proxy_keys.txt'):
        """Устанавливает прокси."""
        # прокси и ключи должны быть разделены тройным дооеточием - :::
        with open(file, 'r') as file:
            for line in file:
                address, key = line.rstrip('\n').split(':::')
                user_agent = self.ua.random
                self.proxies.append(self.Proxy(address, key, user_agent))

    def get_proxies(self) -> list['ProxyHandler.Proxy']:
        return self.proxies