import logging
import random
from python_modules.script_control import ScriptControl
from YandexImagesParser.ImageParser import YandexImage
import pandas as pd
import os
from time import sleep
from threading import Thread, Event, Lock
from queue import Queue
import requests
import uuid
import platform
from python_modules.proxy import ProxyMaster
from pandas import Series
from typing import Optional


logger = logging.Logger(__name__)

logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler()
basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : : %(lineno)d %(message)s')
sh.setFormatter(basic_formater)
logger.addHandler(sh)


class Parser:
    def __init__(self):
        self._lock = Lock()
        self._was_keyword = set()
        self._was_url = set()
        self._checking_url = set()
        self._parser = YandexImage()
        self._queue = Queue()
        self._row_gen  = self._get_row_generator()
        os_name = platform.system()
        # if os_name == 'Windows':
        #     self._result_folder = 'result'
        # elif os_name == 'Linux':
        self._result_folder = 'result'
        if not os.path.exists(self._result_folder):
            os.mkdir(self._result_folder)

        proxy_master = ProxyMaster()
        self._proxies = proxy_master.get_proxy()

    def _get_folders_with_files(self):
        result_folder = self._result_folder
        folders = os.listdir(result_folder)
        folders = list(filter(lambda folder: os.listdir(os.path.join(result_folder, folder)), folders))
        return folders



    def _parse_row(self, row, proxy):
        full_folders = self._get_folders_with_files()
        base_keyword = row['Запрос'].replace(' ', '_')
        if base_keyword in full_folders:
            return
        keywords = row['Запрос из Яндекс.Картинки'].split('\n')
        for keyword in keywords:
            if keyword in self._was_keyword:
                return
            self._was_keyword.add(keyword)
            logger.debug(keyword)
            if not keyword: continue
            while True:
                result = self._parser.search(keyword, self._parser.size.large, proxy)
                if not result:
                    logger.error('Проблемы с парсингом')
                    pause = 600
                    logger.debug('Пауза основного парсера из-за пустого ответа: %s', pause)
                    sleep(pause)
                else:
                    break

            for item in result:
                url = item.url
                x, y = item.width, item.height
                if x > y:
                    x, y = y, x
                proportion = x / y
                if proportion > 4 / 7:
                    if url in self._was_url:
                        continue
                    self._was_url.add(url)
                    if url not in self._checking_url:
                        path = os.path.join(self._result_folder, base_keyword)
                        if not os.path.exists(path):
                            os.makedirs(path)
                        full_path = os.path.join(path, f'{uuid.uuid4()}.jpg')
                        self._queue.put((url, full_path))
                        self._checking_url.add(url)
            pause = random.randint(1, 300)
            logger.debug('Пауза запроса: %s', pause)
            sleep(pause)


    @staticmethod
    def __get_row(df):
        for n, row in df.iterrows():
            yield row



    def _get_row_generator(self, file = 'analitics_clear.csv'):
        df = pd.read_csv(file, sep=',', encoding='utf8')
        df = df[df['Запрос из Яндекс.Картинки'].notna()]
        df = df[df['Запрос из Яндекс.Картинки'].str.lower().str.strip() != 'нет']
        df = df[df['Запрос из Яндекс.Картинки'].str.lower().str.strip() != 'стоп']
        gen = self.__get_row(df)
        return gen


    def _get_row(self):
        try:
            self._lock.acquire()
            row = next(self._row_gen)
            self._lock.release()
            return row
        except StopIteration:
            return None


    def _worker(self, proxy):
        while True:
            row: Optional[Series] = self._get_row()
            if row is not None:
                self._parse_row(row, proxy)
            else:
                logger.debug('Итератор завершил работу')




    def _producer(self):
        workers = []
        for proxy in self._proxies:
            worker = Thread(target=self._worker, args=(proxy,))
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()












        self._stopper.set()

    @staticmethod
    def make_request_with_retries(url, max_retries=20):
        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()  # raise an exception if the status is not 200
                return response.content  # read response content
            except requests.exceptions.RequestException as exc:
                if attempt < max_retries - 1:  # if it's not the last attempt
                    sleep(random.randint(10, 150))  # wait for 3 seconds before the next attempt
                    continue
                else:
                    logger.error(exc)
                    return None

    def save_image(self, url, path):
        logger.debug(f'{path} - {url}')
        image_content = self.make_request_with_retries(url)
        if image_content:
            with open(path, 'wb') as file:
                file.write(image_content)

    def start(self):
        self._stopper = Event()
        producer_thread = Thread(target=self._producer)
        threads = []
        producer_thread.start()
        threads.append(producer_thread)
        while True:
            if not self._queue.empty():
                task = self._queue.get()
                url, path = task
                parser_thread = Thread(target=self.save_image, args=(url, path,))
                parser_thread.start()
                threads.append(parser_thread)
            else:
                if self._stopper.is_set():
                    break

        for thread in threads:
            thread.join()


if __name__ == '__main__':
    with ScriptControl():
        p = Parser()
        p.start()

