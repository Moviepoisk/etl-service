import json
import logging
import os

import psycopg2
import psycopg2.extras
import requests
from backoff import backoff
from dotenv import load_dotenv

load_dotenv()

ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://127.0.0.1:9200")
index_name_film_work = "movies"
index_name_genre = "genres"  # Новый индекс для жанров


@backoff()
def get_connection():
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DATABASE_USER = os.getenv("DATABASE_USER")
    DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
    DATABASE_HOST = os.getenv("DATABASE_HOST")
    DATABASE_PORT = os.getenv("DATABASE_PORT")

    dsl = {
        "dbname": DATABASE_NAME,
        "user": DATABASE_USER,
        "password": DATABASE_PASSWORD,
        "host": DATABASE_HOST,
        "port": int(DATABASE_PORT),
    }
    connection = psycopg2.connect(**dsl)
    return connection


def fetch_and_send_genres_to_elasticsearch(connection):
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Выборка данных по жанрам из базы данных
            cursor.execute(
                """
                SELECT
                    id,
                    name,
                    description
                FROM content.genre;
            """
            )
            genres_data = cursor.fetchall()

        genres_bulk_data = []
        for genre_data in genres_data:
            # Форматирование данных о жанре для отправки в Elasticsearch
            formatted_genre_data = {
                "uuid": str(genre_data["id"]),
                "name": genre_data["name"],
                "description": genre_data["description"],
                # Добавьте другие поля при необходимости
            }
            index_action = {
                "index": {"_id": str(genre_data["id"]), "_index": index_name_genre}
            }
            genres_bulk_data.append(json.dumps(index_action))
            genres_bulk_data.append(json.dumps(formatted_genre_data))

        genres_bulk_payload = "\n".join(genres_bulk_data) + "\n"
        genres_url = f"{ELASTICSEARCH_URL}/_bulk"
        headers = {"Content-Type": "application/x-ndjson"}
        requests.post(genres_url, data=genres_bulk_payload, headers=headers)
        logging.info("Жанры успешно обработаны")

    except Exception as e:
        logging.error(f"Ошибка при получении и отправке данных по жанрам: {e}")


def fetch_and_send_film_works_to_elasticsearch(connection):
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("select min(updated_at) FROM content.film_work;")
            timestamp = cursor.fetchone()[0]

            batch_size = 100
            offset = 0

            while True:
                logging.info("Чтение записей в postgres")
                cursor.execute(
                    f"""
                    SELECT
                    fw.id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    fw.created_at,
                    fw.updated_at,
                    COALESCE (
                        json_agg(
                            DISTINCT jsonb_build_object(
                                'person_role', pfw.role,
                                'person_id', p.id,
                                'person_name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null),
                        '[]'
                    ) as persons,
                    array_agg(DISTINCT g.name) as genres
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    -- WHERE fw.modified >= {timestamp.isoformat()}
                    GROUP BY fw.id
                    ORDER BY fw.updated_at
                    LIMIT {batch_size}
                    OFFSET {offset};
                            """
                )

                datas = cursor.fetchall()
                if not datas:
                    break

                bulk_data = []
                for i, data in enumerate(datas):
                    formatted_data = {
                        "id": data["id"],
                        "imdb_rating": float(data["rating"])
                        if data["rating"] is not None
                        else None,
                        "genre": data["genres"],
                        "title": data["title"],
                        "description": data["description"],
                        "director": [
                            person["person_name"]
                            for person in data["persons"]
                            if person["person_role"] == "director"
                        ],
                        "actors_names": [
                            person["person_name"]
                            for person in data["persons"]
                            if person["person_role"] == "actor"
                        ],
                        "writers_names": [
                            person["person_name"]
                            for person in data["persons"]
                            if person["person_role"] == "writer"
                        ],
                        "actors": [
                            {"id": person["person_id"], "name": person["person_name"]}
                            for person in data["persons"]
                            if person["person_role"] == "actor"
                        ],
                        "writers": [
                            {"id": person["person_id"], "name": person["person_name"]}
                            for person in data["persons"]
                            if person["person_role"] == "writer"
                        ],
                    }
                    index_action = {
                        "index": {"_id": data["id"], "_index": index_name_film_work}
                    }
                    bulk_data.append(json.dumps(index_action))
                    bulk_data.append(json.dumps(formatted_data))
                logging.info("Запись в Elastic Search")

                bulk_payload = "\n".join(bulk_data) + "\n"
                url = f"{ELASTICSEARCH_URL}/_bulk"
                headers = {"Content-Type": "application/x-ndjson"}
                response = requests.post(url, data=bulk_payload, headers=headers)

                logging.info(f"Обработано записей: {offset}")
                offset += batch_size

    except Exception as e:
        logging.error(f"Ошибка: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info("ETL запущен")
    try:
        logging.info("Попытка подключиться к Postgres")
        connection = get_connection()
        logging.info("Подключение успешно")

        # Получение и отправка данных по жанрам
        logging.info("Получение и отправка данных по жанрам в Elasticsearch")
        fetch_and_send_genres_to_elasticsearch(connection)

        # Получение и отправка данных по фильмам
        logging.info("Чтение и отправка данных по фильмам в Elasticsearch")
        fetch_and_send_film_works_to_elasticsearch(connection)

    except Exception as e:
        logging.error(f"Ошибка в процессе ETL: {e}")
    finally:
        if connection:
            connection.close()
            logging.warning("Соединение с Postgres закрыто из-за завершения программы.")
