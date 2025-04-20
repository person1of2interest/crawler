import warnings
import logging

from urllib.parse import urljoin
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from w3lib.url import url_query_cleaner

import asyncio
import aiohttp

from collections import deque

import csv


warnings.filterwarnings(
    "ignore",
    category=XMLParsedAsHTMLWarning
)

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("logs/crawler.log"),  # сохраняем логи в файл
        logging.StreamHandler()  # также выводим в консоль
    ]
)

def is_document(path: str) -> bool:
    """
    По ссылке определяем, указывает
    ли она на документ

    Параметры:
    ----------
        path (str): ссылка

    Возращает:
    ----------
        True/False
    """
    if (path.endswith('.doc') or path.endswith('.docx')
        or path.endswith('.pdf')):
        return True
    else:
        return False


class Crawler:

    def __init__(self, home_domain='spbu.ru', batch_size=8):
        self.home_domain = home_domain # страница, с которой стартуем
        self.visited_urls = set(['https://' + home_domain])
        self.urls_to_visit = deque(['https://' + home_domain])
        self.unique_ext_links = set() # внешние ссылки
        self.ext_links_count = 0 # число внешних ссылок
        self.dead_links_count = 0 # число неработающих страниц
        self.links_to_docs = set() # ссылки на документы
        self.batch_size = batch_size # для асинхронного вызова get

    async def download_url(
        self,
        session: aiohttp.ClientSession,
        url: str
    ) -> str | None:
        """
        Подгружаем веб-страницу

        Параметры:
        ----------
            session (aiohttp.ClientSession): сессия для get
            url (str): загружаемый адрес

        Возвращает:
        -----------
            str | None: текст страницы или 
                        None, если недоступна
        """
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            logging.error(f'Error downloading {url}: {e}')
            self.dead_links_count += 1
            return None

    def get_linked_urls(self, url: str, html: str) -> str:
        """
        По одной выдаем исходящие
        с веб-страницы ссылки

        Параметры:
        ----------
            url (str): адрес страницы
            html (str): текст страницы

        Возвращает:
        -----------
            path (str): адрес ссылки
        """
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            path = link.get('href')
            if path and path.startswith('/'):
                path = urljoin(url, path)
            yield path

    def add_url_to_visit(self, url: str):
        """
        Добавляем ссылку в очередь,
        если еще не посещали и 
        в очереди ее нет

        Параметры:
        ----------
            url (str): ссылка
        """
        url_st = url.rstrip('/')
        if url_st not in self.visited_urls and url_st not in self.urls_to_visit:
            if self.home_domain in url_st:
                self.urls_to_visit.append(url_st)
            else:
                self.ext_links_count += 1
                self.unique_ext_links.add(url_st)

    async def crawl(self, session: aiohttp.ClientSession, url: str):
        """
        Обрабатываем страницу

        Параметры:
        ----------
            session (aiohttp.ClientSession): сессия для get
            url (str): загружаемый адрес
        """
        if is_document(url):
            self.links_to_docs.add(url)
        else:
            html = await self.download_url(session, url)
            if html:
                for url in self.get_linked_urls(url, html):
                    if url:
                        self.links_count += 1
                        self.add_url_to_visit(url)
    
    async def run(self, max_hops: int = 500):
        """
        Запускаем crawler
        """
        async with aiohttp.ClientSession() as session:
            with open('logs/links.txt', 'w') as outfile:
                self.links_count = 1
                while self.urls_to_visit and len(self.visited_urls) < max_hops:
                    # собираем батч из адресов
                    batch = []
                    for _ in range(min(self.batch_size, len(self.urls_to_visit))):
                        # удаляем мета-теги
                        url = url_query_cleaner(self.urls_to_visit.popleft())
                        batch.append(url)

                    logging.info(f'Processing batch: {batch}')
                    
                    # обрабатываем батч асинхронно
                    tasks = [self.crawl(session, url) for url in batch]
                    await asyncio.gather(*tasks)

                    # добавляем адреса в список посещенных
                    self.visited_urls.update(batch)

                    # загружаем адреса из батча в файл
                    outfile.write('\n'.join(batch) + '\n')
