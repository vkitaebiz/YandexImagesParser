import concurrent.futures
import logging
import random
import threading

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
        self._who_attempt_rows = []
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
        self._workers_threads = []
        self._parsers_threads = []



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
            result = self._parser.search(keyword, self._parser.size.large, proxy)
            if not result:
                self._who_attempt_rows.append(row)
                return

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



    def __get_row(self, df):
        for n, row in df.iterrows():
            yield row
        while True:
            if self._who_attempt_rows:
                yield self._who_attempt_rows.pop()
            else:
                break


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
                break


    def _producer(self):
        for proxy in self._proxies:
            worker = Thread(target=self._worker, args=(proxy,))
            worker.start()
            self._workers_threads.append(worker)

        for worker in self._workers_threads:
            worker.join()

        self._stopper.set()

    @staticmethod
    def make_request_with_retries(url, proxy = None, max_retries=20):
        for attempt in range(max_retries):
            try:
                if not proxy:
                     response = requests.get(url)
                else:
                    response = requests.get(url, proxies = {'http': proxy, 'https': proxy})
                response.raise_for_status()  # raise an exception if the status is not 200
                return response.content  # read response content
            except Exception as exc:
                if attempt < max_retries - 1:  # if it's not the last attempt
                    sleep(random.randint(10, 150))  # wait for 3 seconds before the next attempt
                    continue
                else:
                    logger.error(exc)
                    return None

    def save_image(self, url, path):
        logger.debug(f'{path} - {url}')
        proxy = random.shuffle(self._proxies)
        image_content = self.make_request_with_retries(url, proxy)
        if image_content:
            with open(path, 'wb') as file:
                file.write(image_content)

    def start(self):
        self._stopper = Event()
        producer_thread = Thread(target=self._producer)
        producer_thread.start()
        monitor_thread = Thread(target=self._print_thread_counts)  # Новый поток для мониторинга
        monitor_thread.start()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self._proxies)) as executor:
            while True:
                if not self._queue.empty():
                    task = self._queue.get()
                    url, path = task
                    executor.submit(self.save_image, url, path)
                else:
                    if self._stopper.is_set():
                        break

        for thread in self._parsers_threads:
            thread.join()

        monitor_thread.join()
        producer_thread.join()

    def _print_thread_counts(self):
        while not self._stopper.is_set():
            sleep(1)
            logger.info('Всего потоков: %s', threading.active_count())













if __name__ == '__main__':
    with ScriptControl():
        p = Parser()
        p.start()

