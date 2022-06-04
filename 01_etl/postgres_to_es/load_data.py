import os
import time
import logging
import psycopg2.extras
from dotenv import load_dotenv
from redis import Redis
from etl import ETLProcess
from state import State, RedisStorage

load_dotenv()


def load_data(url: str, port: int, table_name: str) -> None:
    """Основная функция переноса всех данных из PostgreSQL в Elasticsearch"""
    # for state_name in state_names:
    while True:
        data, state = etl.extract(table_name)
        if len(data) == 0:
            return
        etl.loader(data, url, port, state)


if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO)
    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT')
    }
    elastic_host = os.environ.get('ELASTIC_HOST', 'localhost')
    elastic_url = f'http://{elastic_host}'
    elastic_port = os.environ.get('ELASTIC_PORT', 9200)
    time_sleep = int(os.environ.get('TIME_SLEEP', 100))
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', '6379')
    file_storage = RedisStorage(Redis(
        host=redis_host, port=redis_port, db=0))
    table_names = ['film_work', 'person', 'genre']
    states = State(storage=file_storage)
    if states.get_state('modified') is None:
        states.set_state('modified', '1000-04-10')
    while True:
        with psycopg2.connect(
                **dsl, cursor_factory=psycopg2.extras.RealDictCursor
        ) as pg_conn:
            etl = ETLProcess(pg_conn, states)
            for name in table_names:
                load_data(elastic_url, elastic_port, name)
                logger.info(f'successful genre {name} transfer')
        logger.info('successful transfer of all tables')
        pg_conn.close()
        time.sleep(time_sleep)
