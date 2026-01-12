from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

with DAG(
    dag_id="google_books_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "books"]
):

    @task
    def extract():
        url = "https://www.googleapis.com/books/v1/volumes?q=subject:technology&maxResults=40"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()["items"]

    @task
    def transform(items):
        records = []
        for item in items:
            v = item.get("volumeInfo", {})
            records.append((
                item.get("id"),
                v.get("title"),
                ",".join(v.get("authors", [])),
                v.get("publisher"),
                v.get("publishedDate"),
                v.get("averageRating"),
                v.get("ratingsCount"),
                v.get("language")
            ))
        return records

    @task
    def load(records):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS books (
                book_id TEXT PRIMARY KEY,
                title TEXT,
                authors TEXT,
                publisher TEXT,
                published_date TEXT,
                average_rating FLOAT,
                ratings_count INT,
                language TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.executemany("""
            INSERT INTO books (
                book_id, title, authors, publisher,
                published_date, average_rating,
                ratings_count, language
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (book_id) DO NOTHING
        """, records)

        conn.commit()
        cur.close()
        conn.close()

    load(transform(extract()))
