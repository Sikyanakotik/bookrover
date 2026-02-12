'''
main.py: The main entry point for the scraper. Contains code to fetch book data
from the Hardcover API and store it in the database.
'''

import os
import sys
from time import sleep
import re
import requests
import psycopg
from psycopg import sql
from json import JSONDecodeError

# Add the workspace root to the path so imports work regardless of where the script is run from
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from shared_python.src import loadenv
from shared_python.src import db_queries

def reset_database() -> None:
    username = loadenv.loadEnvVariable("POSTGRES_USERNAME")
    password = loadenv.loadEnvVariable("POSTGRES_PASSWORD")
    host = loadenv.loadEnvVariable("POSTGRES_HOST")
    port = loadenv.loadEnvVariable("POSTGRES_PORT")
    
    with psycopg.connect(f'user={username} password={password} host={host} port={port} dbname=bookrover') as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("DROP TABLE IF EXISTS books")
            cur.execute("DROP TABLE IF EXISTS populate_books_progress")
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
                    description TEXT,
                    languages TEXT[]
                )
            """)
            cur.execute("""
                        CREATE TRIGGER set_timestamp
                        BEFORE UPDATE ON books
                        FOR EACH ROW
                        EXECUTE FUNCTION trigger_set_timestamp()
                        """)
            cur.execute("""
                CREATE TABLE populate_books_progress (
                    genre TEXT PRIMARY KEY,
                    last_page_fetched INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.execute("""
                        CREATE TRIGGER set_timestamp
                        BEFORE UPDATE ON populate_books_progress
                        FOR EACH ROW
                        EXECUTE FUNCTION trigger_set_timestamp()
                        """)
            conn.commit()
    

def addBooksToDatabase(response: dict) -> None:
    username = loadenv.loadEnvVariable("POSTGRES_USERNAME")
    password = loadenv.loadEnvVariable("POSTGRES_PASSWORD")
    host = loadenv.loadEnvVariable("POSTGRES_HOST")
    port = loadenv.loadEnvVariable("POSTGRES_PORT")
    
    if "data" not in response or "books" not in response["data"]:
        print(f"Unexpected API response format. Response: {response}")
        raise JSONDecodeError("Unexpected API response format", str(response), 0)

    with psycopg.connect(f'user={username} password={password} host={host} port={port} dbname=bookrover') as conn:
        with conn.cursor() as cur:
            for book in response["data"]["books"]:
                # Extract relevant fields from the API response
                hardcover_id = book["id"]
                title = book["title"]
                description = book["description"]
                average_rating = book["rating"]

                authors: list[str] = []
                for contributor in book["contributions"]:
                    if contributor["contribution"] in ["Author", "Editor", None]:
                        authors.append(contributor["author"]["name"])

                num_good_ratings = sum([rating["count"] for rating in book["ratings_distribution"] if rating["rating"] >= 3.5])

                first_edition = book["editions"][0]
                isbn_13 = re.sub(r"[^0-9]", "", first_edition["isbn_13"])
                release_date = first_edition["release_date"]
                language_set: set[str] = set()
                for edition in book["editions"]:
                    if edition != None and edition["language"] != None and edition["language"]["code3"] != None:
                        language_set.add(edition["language"]["code3"])
                languages = list(language_set)

                tags = book["cached_tags"] 
                highest_genre_tag_count = max([tag["count"] for tag in tags["Genre"]], default=0)
                genre_tags = [tag["tag"] for tag in tags["Genre"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_genre_tag_count]
                highest_mood_tag_count = max([tag["count"] for tag in tags["Mood"]], default=0)
                mood_tags = [tag["tag"] for tag in tags["Mood"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_mood_tag_count]
                highest_content_tag_count = max([tag["count"] for tag in tags["Content Warning"]], default=0)
                content_tags = [tag["tag"] for tag in tags["Content Warning"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_content_tag_count]

                if any(tag in genre_tags for tag in [
                    "Comics", "Graphic Novels", "Comics & Graphic Novels",
                    "Manga", "Manhwa", "Non-Fiction", "Nonfiction", "Biography",
                    "Memoir", "Essays", "Self-Help", "Puzzles", "Textbooks"]):
                    # Bookrover is for prose fiction.
                    continue

                if "Poetry" in genre_tags and ("Epic Poetry" not in genre_tags or "Narrative Poetry" not in genre_tags):
                    # We don't want poetry collections, but narrative poetry is fine.
                    continue

                # Add the book to the database, or update it if it already exists
                cur.execute(sql.SQL("""
                    INSERT INTO books (
                        hardcover_id, isbn_13, title, authors, num_good_ratings,
                        average_rating, release_date, genre_tags, mood_tags,
                        content_tags, description, languages
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hardcover_id) DO UPDATE SET
                        isbn_13 = EXCLUDED.isbn_13,
                        title = EXCLUDED.title,
                        authors = EXCLUDED.authors,
                        num_good_ratings = EXCLUDED.num_good_ratings,
                        average_rating = EXCLUDED.average_rating,
                        release_date = EXCLUDED.release_date,
                        genre_tags = EXCLUDED.genre_tags,
                        mood_tags = EXCLUDED.mood_tags,
                        content_tags = EXCLUDED.content_tags,
                        description = EXCLUDED.description,
                        languages = EXCLUDED.languages
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
                    description,
                    languages
                ))
            conn.commit()


def populateDatabase(genres: list[str] | str | None = None) -> None:
    '''
    Adds books in batches from the Hardcover API to the database, starting with the most 
    popular books in each of ten canonical genres.

    Don't fill the database all at once, or Hardcover will cut us off. Use the scheduler
    (once it's implemented) to add new books on a reasonable schedule.
    '''
    if genres is None:
        genres_to_search = ["Fantasy", "Science Fiction", "Romance", "Thriller", "Mystery",
                            "Young Adult", "Horror", "Juvenile Fiction", "Literary",
                            "Classics"]
    elif isinstance(genres, str):
        genres_to_search = [genres]
    else:
        genres_to_search = genres

    # As much as we'd love to add books that don't match any of these genres, the API
    # doesn't allow us to exclude specific genres in our search. And we need to ensure
    # each of these categories is well-represented in the database.

    db_size = db_queries.getBookCount()
    max_books_db_size = int(loadenv.loadEnvVariable("MAX_BOOKS_DB_SIZE"))
    max_repeated_pages = 5 # Maximum number of pages to try before giving up on a genre,
                           # in case we keep hitting pages with books we've already added
                           # to the database. We don't want to keep hammering the API
                           # with requests that won't add any new books all at once.

    api_url = loadenv.loadEnvVariable("HARDCOVER_API_URL")
    api_key = loadenv.loadEnvVariable("HARDCOVER_API_KEY")
    username = loadenv.loadEnvVariable("POSTGRES_USERNAME")
    password = loadenv.loadEnvVariable("POSTGRES_PASSWORD")
    host = loadenv.loadEnvVariable("POSTGRES_HOST")
    port = loadenv.loadEnvVariable("POSTGRES_PORT")

    for genre in genres_to_search:
        if db_size >= max_books_db_size:
            print(f"Database has reached the maximum size of {max_books_db_size} books. Stopping population.")
            break

        page: int
        with psycopg.connect(f'user={username} password={password} host={host} port={port} dbname=bookrover') as conn:
            with conn.cursor() as cur:    
                query = sql.SQL("SELECT last_page_fetched FROM populate_books_progress WHERE genre = %s").format(sql.Identifier(genre))
                cur.execute(query, (genre,))
                result = cur.fetchone()
                page = int(result[0] if result else 0)

        # Repeat until we add new books to the database, the response is empty, or
        # the API returns an error.
        for _ in range(max_repeated_pages):

            ## Fetch Hardcover IDs for the current genre, ordered by popularity, using
            #  the search API. The API lets us fetch a maximum of 25 books per query.
            page += 1
            query = {"query": """
                query {
                    search(
                        query: "%s"
                        query_type: "Book"
                        per_page: 25   
                        page: %d
                        fields: "genres"
                        weights: "1"
                    ) {
                        ids
                    }
                }
                """ % (genre, page)
            }
            
            request_header = {"content-type": "application/json",
                                "authorization": api_key }
            response = requests.post(api_url, json=query,
                                        headers=request_header)
            if response.status_code != 200:
                print(f"API request failed for genre {genre} on page {page}. Status code: {response.status_code}")                
                print(f"Response: {response.text}")
                print("Terminating population process.")
                return
            
            response_json = response.json()
            # print(f"{response_json=}") # Debugging line - uncomment to see the full API response
            if "data" not in response_json or "search" not in response_json["data"] or "ids" not in response_json["data"]["search"]:
                print(f"Unexpected API response for genre {genre} on page {page}. Response: {response_json}")
                print("Terminating population process.")
                return
            ids: list[int] = response_json["data"]["search"]["ids"]
            if not ids:
                print(f"No more books found for genre {genre} after page {page}. Moving to next genre.")
                page -= 1 # Decrement page to avoid skipping pages in future runs, since we didn't
                          # actually add any new books to the database on this page
                sleep(2) # Keep being nice to the API.
                break
            ids = [int(id) for id in ids] # Ensure all IDs are integers
            print(f"Fetched Hardcover IDs for genre {genre} on page {page}: {len(ids)} books found.")

            sleep(2) # Be nice to the API, and avoid sending too many requests in a 
                     # short time.

            ## Fetch detailed book data for the Hardcover IDs we got back, and add it to
            #  the database.
            #  literary_type_id = 1 restricts the search to fiction books
            query = {"query": '''
                query FetchBooks {
                    books( 
                        where: {literary_type_id: {_eq: 1}, id: {_in: %s}}
                    ) {
                        id
                        title
                        contributions {
                            contribution
                            author {
                                name                            
                            }
                        }
                        description
                        rating
                        ratings_distribution
                        editions(
                            order_by: {release_date: asc}
                            where: {isbn_13: {_is_null: false}, release_date: {_is_null: false}}
                        ) {
                            release_date
                            isbn_13
                            language {
                                code3
                            }
                        }
                        cached_tags
                    }
                }
             ''' % (str(ids),)}
            # print(query) # Debugging line - uncomment to see the full query we're sending to the API

            request_header = {"content-type": "application/json",
                              "authorization": api_key }
            response = requests.post(api_url, json=query,
                                     headers=request_header)

            if response.status_code != 200:
                print(f"API request failed for fetching books in genre {genre}. Status code: {response.status_code}")
                print(f"Response: {response.text}")
                print("Terminating population process.")
                # Stop immediately if we hit an error here, to avoid wasting API requests
                # on bad calls.
                return

            addBooksToDatabase(response.json())
            new_books_added = db_queries.getBookCount() - db_size
            if new_books_added > 0:
                print(f"Added {new_books_added} new books to the database for genre {genre}. Total books in database: {db_queries.getBookCount()}")
                db_size = db_queries.getBookCount() # Update the database size after adding new books
                sleep(2) # Keep being nice to the API.
                break # Move to the next genre after successfully adding new books
            
            print(f"No new books were added to the database for genre {genre} on page {page}.")
            print("Information updated for repeated books. Trying the next page.")
            sleep(2) # Keep being nice to the API. (They've been so nice to us!)

        with psycopg.connect(f'user={username} password={password} host={host} port={port} dbname=bookrover') as conn:
            with conn.cursor() as cur:
                # Update the last_page_fetched for this genre
                cur.execute("""
                    INSERT INTO populate_books_progress (genre, last_page_fetched)
                    VALUES (%s, %s)
                    ON CONFLICT (genre) DO UPDATE SET
                        last_page_fetched = EXCLUDED.last_page_fetched
                """, (genre, page))
                conn.commit()

    print(f"Finished populating database. Total books in database: {db_queries.getBookCount()}")
            

def hardcover_api_test() -> None:
    api_url = loadenv.loadEnvVariable("HARDCOVER_API_URL")
    api_key = loadenv.loadEnvVariable("HARDCOVER_API_KEY")

    query = {"query": '''
                query TopTen {
                    books(order_by: {users_read_count: desc_nulls_last}
                    limit: 10
                    where: {literary_type_id: {_eq: 1}} # Restricts the search to fiction books
                    ) {                                 # Yes, queries respect '#' comments.
                        id
                        title
                        contributions {
                            contribution
                            author {
                                name                            
                            }
                        }
                        description
                        rating
                        ratings_distribution
                        editions(
                            order_by: {release_date: asc}
                            where: {isbn_13: {_is_null: false}, release_date: {_is_null: false}}
                        ) {
                            release_date
                            isbn_13
                            language {
                                code3
                            }
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
        # print(response.json()) # Debugging line - uncomment to see the full API response
        addBooksToDatabase(response.json())

    print(f"Total books in database: {db_queries.getBookCount()}")

def main() -> None:
    # reset_database() # Uncomment this line to reset the database before populating it. Use with caution!
    populateDatabase("LGBT")
    populateDatabase(["Historical", "Humor", "Sports"])

if __name__ == "__main__":
    main()