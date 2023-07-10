import logging
import random
from python_modules.script_control import ScriptControl
from YandexImagesParser.ImageParser import YandexImage
import pandas as pd
import os
from time import sleep
from threading import Thread
from queue import Queue
import requests
import uuid


logger = logging.Logger(__name__)

logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler()
basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s : %(lineno)d')
sh.setFormatter(basic_formater)
logger.addHandler(sh)


class Parser:
    def __init__(self):
        self._parser = YandexImage()

    def producer(self, queue):
        checking_url = set()
        df = pd.read_csv('analytics.csv', sep = ',', encoding='cp1251')
        df = df[df['Чистый запрос'].notna()]
        df = df[df['Чистый запрос'].str.lower().str.strip() != 'нет']
        df = df[df['Чистый запрос'].str.lower().str.strip() != 'стоп']
        was_keyword = set()
        was_url = set()
        for _, row in df.iterrows():
            base_keyword = row['Keyword']
            keywords = row['Чистый запрос'].split('\n')
            for keyword in keywords:
                if keyword in was_keyword:
                    continue
                was_keyword.add(keyword)
                logger.debug( keyword)
                if not keyword: continue
                while True:
                    result = self._parser.search(keyword, self._parser.size.large)
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
                        if url in was_url:
                            continue
                        was_url.add(url)
                        if url not in checking_url:
                            path = os.path.join('result', base_keyword.replace(' ', '_'))
                            if not os.path.exists(path):
                                os.makedirs(path)
                            full_path = os.path.join(path, f'{uuid.uuid4()}.jpg')

                            queue.put((url, full_path))
                            checking_url.add(url)
                pause = random.randint(1, 300)
                logger.debug('Пауза запроса: %s', pause)
                sleep(pause)
        queue.put('stop')

    @staticmethod
    def make_request_with_retries(url, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()  # raise an exception if the status is not 200
                return response.content  # read response content
            except requests.exceptions.RequestException:
                if attempt < max_retries - 1:  # if it's not the last attempt
                    sleep(3)  # wait for 3 seconds before the next attempt
                    continue
                else:
                    logger.debug(f"Failed to make a request to {url} after {max_retries} attempts")
                    return None

    def save_image(self, url, path):
        logger.debug(url)
        image_content = self.make_request_with_retries(url)
        if image_content:
            with open(path, 'wb') as file:
                file.write(image_content)

    def start(self):
        queue = Queue()
        # self.producer(queue)
        producer_thread = Thread(target=self.producer, args=(queue,))
        threads = []
        producer_thread.start()
        threads.append(producer_thread)
        while True:
            if not queue.empty():
                task = queue.get()
                if task == 'stop':
                    break
                url, path = task
                parser_thread = Thread(target=self.save_image, args=(url, path,))
                parser_thread.start()
                threads.append(parser_thread)

        for thread in threads:
            thread.join()


if __name__ == '__main__':
    with ScriptControl():
        p = Parser()
        p.start()

