import os
import requests
import psycopg
from psycopg import sql

from dotenv import load_dotenv
load_dotenv()

def loadEnvVariable(name: str) -> str:
    value = os.getenv(name)
    if value == None:
        raise EnvironmentError(f"{name} not found in environment.")
    return value

def reset_database() -> None:
    username = loadEnvVariable("POSTGRES_USERNAME")
    password = loadEnvVariable("POSTGRES_PASSWORD")
    host = loadEnvVariable("POSTGRES_HOST")
    port = loadEnvVariable("POSTGRES_PORT")
    
    with psycopg.connect(f'user={username} password={password} host={host} port={port} dbname=bookrover') as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("DROP TABLE IF EXISTS books")
            cur.execute("""
                        CREATE OR REPLACE FUNCTION trigger_set_timestamp()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW.updated_at = NOW();
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql
                        """)
            cur.execute("""
                CREATE TABLE books (
                    id BIGSERIAL PRIMARY KEY,
                    isbn_13 TEXT UNIQUE NOT NULL,
                    hardcover_id BIGINT UNIQUE NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    title TEXT NOT NULL,
                    authors TEXT[],
                    num_good_ratings INTEGER NOT NULL,
                    average_rating FLOAT,
                    release_date DATE NOT NULL,
                    genre_tags TEXT[],
                    mood_tags TEXT[],
                    content_tags TEXT[],
                    description TEXT
                )
            """)
            cur.execute("""
                        CREATE TRIGGER set_timestamp
                        BEFORE UPDATE ON books
                        FOR EACH ROW
                        EXECUTE FUNCTION trigger_set_timestamp()
                        """)
            conn.commit()
    
def addBooksToDatabase(response: dict) -> None:
    username = loadEnvVariable("POSTGRES_USERNAME")
    password = loadEnvVariable("POSTGRES_PASSWORD")
    host = loadEnvVariable("POSTGRES_HOST")
    port = loadEnvVariable("POSTGRES_PORT")
    
    with psycopg.connect(f'user={username} password={password} host={host} port={port} dbname=bookrover') as conn:
        with conn.cursor() as cur:
            for book in response["data"]["books"]:
                hardcover_id = book["id"]
                title = book["title"]
                description = book["description"]
                average_rating = book["rating"]

                authors: list[str] = []
                for contributor in book["cached_contributors"]:
                    authors.append(contributor["author"]["name"])

                num_good_ratings = sum([rating["count"] for rating in book["ratings_distribution"] if rating["rating"] >= 3.5])

                first_edition = book["editions"][0]
                isbn_13 = first_edition["isbn_13"]
                release_date = first_edition["release_date"]

                tags = book["cached_tags"] 
                highest_genre_tag_count = max([tag["count"] for tag in tags["Genre"]], default=0)
                genre_tags = [tag["tag"] for tag in tags["Genre"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_genre_tag_count]
                highest_mood_tag_count = max([tag["count"] for tag in tags["Mood"]], default=0)
                mood_tags = [tag["tag"] for tag in tags["Mood"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_mood_tag_count]
                highest_content_tag_count = max([tag["count"] for tag in tags["Content Warning"]], default=0)
                content_tags = [tag["tag"] for tag in tags["Content Warning"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_content_tag_count]

                cur.execute(sql.SQL("""
                    INSERT INTO books (
                        hardcover_id, isbn_13, title, authors, num_good_ratings,
                        average_rating, release_date, genre_tags, mood_tags,
                        content_tags, description
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """), (
                    hardcover_id,
                    isbn_13,
                    title,
                    authors,
                    num_good_ratings,
                    average_rating,
                    release_date,
                    genre_tags,
                    mood_tags,
                    content_tags,
                    description
                ))
            conn.commit()


def hardcover_api_test() -> None:
    api_url = os.getenv("HARDCOVER_API_URL")
    if api_url == None:
        raise EnvironmentError("HARDCOVER_API_URL not found in environment.")
    api_key = os.getenv("HARDCOVER_API_KEY")
    if api_key == None:
        raise EnvironmentError("HARDCOVER_API_KEY not found in environment.")

    query = {"query": '''
                query TopTen {
                    books(order_by: {users_read_count: desc_nulls_last}, limit: 10) {
                        id
                        title
                        cached_contributors
                        description
                        rating
                        ratings_distribution
                        editions(
                            limit: 1
                            order_by: {release_date: asc}
                            where: {isbn_13: {_is_null: false}, release_date: {_is_null: false}}
                        ) {
                            release_date
                            isbn_13
                        }
                        cached_tags
                    }
                }
             '''}

    request_header = {"content-type": "application/json",
                      "authorization": api_key }
    response = requests.post(api_url, json=query,
                             headers=request_header)

    print(f"{response.status_code=}")
    if response.status_code != 200:
        print("API request failed.")
    else:
        addBooksToDatabase(response.json())

def main() -> None:
    reset_database()
    hardcover_api_test()

if __name__ == "__main__":
    main()