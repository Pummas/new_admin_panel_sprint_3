import json
import requests
import psycopg2.extensions
from state import State
from postgres_loader import PostgresLoader


class ETLProcess:
    """
    Класс для переноса данных из PostgreSQL в Elasticsearch
    """
    def __init__(self, pg_conn: psycopg2.extensions.connection, state: State):
        self.states = state
        self.postgres_loader = PostgresLoader(pg_conn)
        self.state = self.states.get_state('modified')

    def extract(self):
        """Взять состояние и учитывая состояние
        получить данные из PostgreSQL"""
        self.state = self.states.get_state('modified')
        if self.state is None:
            self.states.set_state('modified', '2021-04-10')
            self.state = self.states.get_state('modified')
        data = self.postgres_loader.load_data(self.state)
        return data

    def transform(self, data: dict):
        """Преобразовать данные в нужный формат"""
        request = json.dumps(
            {"index": {"_index": "movies", "_id": data["id"]}}
        )
        return f"{request}\n {json.dumps(data)} \n", data.pop('modified')

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
