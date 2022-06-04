import json
import time
from functools import wraps
from logging import getLogger

import psycopg2.extensions
import requests

from postgres_loader import PostgresLoader
from state import State


def backoff(logger, start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции
    через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 1
            sleep_time = start_sleep_time * factor ** n
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.error(e)
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
        self.state = ''

    @backoff(logger=getLogger())
    def extract(self, table_name: str) -> tuple:
        """Взять состояние и учитывая состояние
        получить данные из PostgreSQL"""
        try:
            self.state = self.states.get_state('modified')
            if table_name == 'person':
                data = self.postgres_loader.load_person(self.state)
            elif table_name == 'genre':
                data = self.postgres_loader.load_genre(self.state)
            else:
                data = self.postgres_loader.load_film_work(self.state)
            if len(data) == 0:
                return data, self.state
            films_id, state = self.postgres_loader.get_films_id(data)
            data_to_transform = self.postgres_loader.load_data(
                str(films_id).replace("[", "(").replace("]", ")"))
            return data_to_transform, state
        except Exception as e:
            raise e

    def transform(self, data: dict) -> str:
        """Преобразовать данные в нужный формат"""
        request = json.dumps(
            {"index": {"_index": "movies", "_id": data["id"]}}
        )
        return f"{request}\n {json.dumps(data)} \n"

    @backoff(logger=getLogger())
    def loader(self, data: list, url: str, port: int, state: str):
        """Загрузить данные в Elasticsearch и обновить состояние"""
        transform_data = ''
        for elem in data:
            transform_elem = self.transform(elem)
            transform_data += transform_elem
        bulk_url = f'{url}:{port}/_bulk'
        requests.post(
            bulk_url,
            data=transform_data,
            headers={'content-type': 'application/json', 'charset': 'UTF-8'}
        )
        self.states.set_state('modified', state)
