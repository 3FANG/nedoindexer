import asyncio
import logging

import aiofiles


class AsyncFileHandler(logging.Handler):
    """Асинхронный файловый обработчик для пакета ведения журналов logging."""

    def __init__(self, filename: str='logger.log') -> None:
        super().__init__()
        self.filename = filename

    async def emit_async(self, record):
        """Выполнить запись в файл в асинхронном режиме."""
        msg = self.format(record)
        async with aiofiles.open(self.filename, 'a') as file:
            await file.write(msg + '\n')

    def emit(self, record):
        """Cоздать асинхронную задачу для записи лога."""
        asyncio.create_task(self.emit_async(record))