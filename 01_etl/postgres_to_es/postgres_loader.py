from logging import getLogger
import psycopg2


class PostgresLoader:

    def __init__(self, pg_conn: psycopg2.extensions.connection):
        self._connection = pg_conn
        self._cursor = self._connection.cursor()
        self._logger = getLogger()
        self.batch_size = 100

    def load_data(self, state):
        try:
            self._cursor.execute(
                f"""
SELECT fw.id,
fw.rating as imdb_rating,
array_agg(DISTINCT g.name) as genre,
fw.title,
fw.description,
array_remove(array_agg(DISTINCT d.full_name), null) as director,
array_remove(array_agg(DISTINCT p.full_name), null)   as actors_names,
array_remove(array_agg(DISTINCT w.full_name), null)  as writers_names,
COALESCE(
json_agg(
DISTINCT jsonb_build_object(
'id', p.id,
'name', p.full_name
)
) FILTER (WHERE p.id is not null),
'[]'
) as actors,
COALESCE(
json_agg(
DISTINCT jsonb_build_object(
'id', w.id,
'name', w.full_name
)
) FILTER (WHERE w.id is not null),
'[]'
) as writers,
to_char(fw.modified, 'YYYY-MM-DD HH24:MI:SS.US') as modified
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id and role = 'actor'
LEFT JOIN content.person d ON d.id = pfw.person_id and role = 'director'
LEFT JOIN content.person w ON w.id = pfw.person_id and role = 'writer'
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified > '{state}'
GROUP BY fw.id,
fw.title,
fw.description,
fw.rating,
fw.type,
fw.created,
fw.modified
ORDER BY fw.modified
LIMIT {self.batch_size}
""")  # noqa: S608
            # while True:
            #     rows = self._cursor.fetchmany(size=self.batch_size)
            return self._cursor.fetchall()
            # if not rows:
            #     return
            # yield from rows
        except Exception as e:
            raise e
