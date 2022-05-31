import os
import psycopg2.extras
from logging import getLogger
from dotenv import load_dotenv
from state import State, JsonFileStorage
from etl import ETLProcess

load_dotenv()


def load_data(pg_conn, states):
    """Основная функция переноса всех данных из PostgreSQL в Elasticsearch"""
    etl = ETLProcess(pg_conn, states)
    while True:
        data = etl.extract()
        if len(data) == 0:
            return
        etl.loader(data)


if __name__ == '__main__':
    logger = getLogger()
    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT')
    }
    file_storage = JsonFileStorage('station.txt')
    states = State(storage=file_storage)
    with psycopg2.connect(
            **dsl, cursor_factory=psycopg2.extras.RealDictCursor
    ) as pg_conn:
        load_data(pg_conn, states)
    pg_conn.close()
