import os
from logging import getLogger

import psycopg2
from dotenv import load_dotenv

load_dotenv()


class PostgresLoader:
    """
    Класс, реализующий запросы к PostgreSQL
    """

    def __init__(self, pg_conn: psycopg2.extensions.connection):
        self._connection = pg_conn
        self._cursor = self._connection.cursor()
        self._logger = getLogger()

    def load_data(self, films_works_id: str) -> list:
        """Выдать ограниченное число фильмов, жанров и актеров
        отсортированных по дате изменения фильмов"""
        try:
            self._cursor.execute(
                f"""
SELECT fw.id,
fw.rating as imdb_rating,
array_agg(DISTINCT g.name) as genre,
fw.title,
fw.description,
array_remove(array_agg(DISTINCT p.full_name)
    FILTER ( WHERE role = 'actor' ),  null)   as actor,
array_remove(array_agg(DISTINCT p.full_name)
    FILTER ( WHERE role = 'director' ), null) as director,
array_remove(array_agg(DISTINCT p.full_name)
    FILTER ( WHERE role = 'writer' ), null)  as writer,
  COALESCE(
           json_agg(
           DISTINCT jsonb_build_object(
                   'id', p.id,
                   'name', p.full_name
               )
       ) FILTER (WHERE p.id is not null),
           '[]'
)                                            as actors,
COALESCE(
           json_agg(
           DISTINCT jsonb_build_object(
                   'id', p.id,
                   'name', p.full_name
               )
       ) FILTER (WHERE p.id is not null and role = 'writer'),
           '[]'
)                                            as writers
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.id IN {films_works_id}
GROUP BY fw.id
""")  # noqa: S608
            return self._cursor.fetchall()
        except Exception as e:
            raise e

    def get_films_id(self, data: list) -> tuple:
        try:
            films_id = []
            for elem in data:
                films_id.append(elem.get('id'))
            return films_id, data[-1].get('modified')
        except Exception as e:
            raise e

    def load_film_work(self, state: str):
        try:
            self._cursor.execute(
                f"""
                SELECT
                    id,
                    to_char(modified, 'YYYY-MM-DD HH24:MI:SS.US')
                    as modified
                FROM content.film_work
                WHERE modified > '{state}'
                LIMIT {os.environ.get('BATCH_SIZE')};
                """)  # noqa: S608
            return self._cursor.fetchall()
        except Exception as e:
            raise e

    def load_person(self, state: str):
        try:
            self._cursor.execute(
                f"""
                SELECT
                    film_work_id as id,
                    to_char(person.modified, 'YYYY-MM-DD HH24:MI:SS.US')
                    as modified
                FROM content.person
                JOIN content.person_film_work pfw on person.id = pfw.person_id
                WHERE modified > '{state}'
                LIMIT {os.environ.get('BATCH_SIZE')};
                """)  # noqa: S608
            return self._cursor.fetchall()
        except Exception as e:
            raise e

    def load_genre(self, state: str):
        try:
            self._cursor.execute(
                f"""
                SELECT
                    film_work_id as id,
                    to_char(genre.modified, 'YYYY-MM-DD HH24:MI:SS.US')
                    as modified
                FROM content.genre
                JOIN  content.genre_film_work gfw on genre.id = gfw.genre_id
                WHERE modified > '{state}'
                LIMIT {os.environ.get('BATCH_SIZE')};
                """)  # noqa: S608
            return self._cursor.fetchall()
        except Exception as e:
            raise e
