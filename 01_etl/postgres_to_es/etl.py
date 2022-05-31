import json
import time
from functools import wraps
from logging import getLogger

import psycopg2.extensions
import requests

from postgres_loader import PostgresLoader
from state import State


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции
    через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 1
            sleep_time = start_sleep_time * factor ** n
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if sleep_time < border_sleep_time:
                        time.sleep(sleep_time)
                        n += 1
                        sleep_time = start_sleep_time * factor ** n
                    else:
                        time.sleep(border_sleep_time)
        return inner

    return func_wrapper


class ETLProcess:
    """
    Класс для переноса данных из PostgreSQL в Elasticsearch
    """

    def __init__(self, pg_conn: psycopg2.extensions.connection, state: State):
        self.states = state
        self.postgres_loader = PostgresLoader(pg_conn)
        self.state = self.states.get_state('modified')
        self.logger = getLogger()

    @backoff()
    def extract(self):
        """Взять состояние и учитывая состояние
        получить данные из PostgreSQL"""
        try:
            self.state = self.states.get_state('modified')
            if self.state is None:
                self.states.set_state('modified', '1000-04-10')
                self.state = self.states.get_state('modified')
            data = self.postgres_loader.load_data(self.state)
            self.logger.info('Success')
            return data
        except Exception as e:
            self.logger.error(e)
            raise e

    def transform(self, data: dict):
        """Преобразовать данные в нужный формат"""
        request = json.dumps(
            {"index": {"_index": "movies", "_id": data["id"]}}
        )
        return f"{request}\n {json.dumps(data)} \n", data.pop('modified')

    @backoff()
    def loader(self, data: list):
        """Загрузить данные в Elasticsearch и обновить состояние"""
        transform_data = ''
        for elem in data:
            transform_elem, state = self.transform(elem)
            transform_data += transform_elem
        url = 'http://localhost:9200/_bulk'
        requests.post(
            url,
            data=transform_data,
            headers={'content-type': 'application/json', 'charset': 'UTF-8'}
        )
        self.states.set_state('modified', state)
