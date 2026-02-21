'''
main.py: The main entry point for the scraper. Contains code to fetch book data
from the Hardcover API and store it in the database.
'''

import os
import sys
import argparse
from datetime import datetime, timezone
from time import sleep
import re
import requests
import psycopg
from psycopg import sql, rows
from json import JSONDecodeError
from nltk.stem import PorterStemmer

# Add the workspace root to the path so imports work regardless of where the script is run from
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from shared_python.src import loadenv
from shared_python.src import db_queries
from shared_python.src import embeddings

# A set of common English stop words, to exclude from the inverted index.
# These are words that are very common and not useful for search, since they don't
# carry much meaning on their own. This list is based on the NLTK English stop words
# list, but can be customized as needed.
STOP_WORDS = set([
    "a", "about", "above", "after", "again", "against", "ain", "ain't",
    "all", "am", "an", "and", "any", "are", "aren", "aren't",
    "as", "at", "be", "because", "been", "before", "being", 
    "below", "between", "both", "but", "by", "can", "could", "couldn", "couldn't",
    "d", "did", "didn", "didn't", "do", "does", "doesn", "doesn't",
    "doing", "dont", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn", "hadn't", "has", "hasn",
    "hasn't", "have", "haven", "haven't", "having", "he", "he'd",
    "he'll", "he's", "her", "here", "hers", "herself", "him",
    "himself", "his", "how", "i", "id" "i'd", "i'll", "i'm", "i've",
    "if", "in", "into", "is", "isn", "isn't", "it", "it'd",
    "it'll", "it's", "its", "itself", "just", "ll", "m", "ma",
    "me", "mightn", "mightn't", "more", "most", "mustn", "mustn't",
    "my", "myself", "needn", "needn't", "no", "nor", "not", "now",
    "o", "of", "off", "on", "once", "only", "or", "other", "our",
    "ours", "ourselves", "out", "over", "own", "re", "s", "same",
    "shan", "shan't", "she", "she'd", "she'll", "she's", "should",
    "should've", "shouldn", "shouldn't", "so", "some", "such", "t",
    "than", "that", "that'll", "the", "their", "theirs", "them",
    "themselves", "then", "there", "these", "they", "they'd",
    "they'll", "they're", "they've", "this", "those", "through",
    "to", "too", "under", "until", "up", "ve", "very", "was",
    "wasn", "wasn't", "we", "we'd", "we'll", "we're", "we've",
    "were", "weren", "weren't", "what", "when", "where", "which",
    "while", "who", "whom", "why", "will", "with", "won", "won't",
    "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you're",
    "you've", "your", "yours", "yourself", "yourselves",
    "b", "c", "d", "e", "f", "g", "h", "j", "k", "l", "m",
    "n", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"
])

def reset_database() -> None:
    model_dimensions = int(loadenv.loadEnvVariable("EMBEDDING_DIMENSIONS"))
    
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("DROP TABLE IF EXISTS populate_books_progress")
            cur.execute("DROP TABLE IF EXISTS book_embeddings")
            cur.execute("DROP TABLE IF EXISTS inverted_index")
            cur.execute("DROP TABLE IF EXISTS books")

            # TABLE books: Stores book metadata.
            #
            # Columns:
            # - id: Primary key.
            # - isbn_13: The book's ISBN-13 number, from the first print edition by date.
            # - hardcover_id: The book's ID in Hardcover's database.
            # - updated_at: Timestamp of the last update to this record. Automatically set
            #               to the current time whenever the record is updated.
            # - title: The book's title.
            # - authors: An array of the book's authors.
            # - num_good_ratings: The number of ratings the book has received with a rating
            #                     of 3.5 or higher, as a proxy for the book's popularity.
            # - average_rating: The book's average rating.
            # - release_date: The release date of the book's first print edition.
            # - genre_tags: An array of the book's genre tags, from Hardcover's user-generated tags.
            #               Only includes tags that are in the "Genre" category, have at least 2
            #               counts, and have at least 20% of the counts of the most popular genre
            #               tag for the book.
            # - mood_tags: An array of the book's mood tags, from Hardcover's user-generated tags.
            #              Only includes tags that are in the "Mood" category, have at least 2
            #              counts, and have at least 20% of the counts of the most popular mood tag
            #              for the book.
            # - content_tags: An array of the book's content warning tags, from Hardcover's
            #                 user-generated tags. Only includes tags that are in the
            #                 "Content Warning" category, have at least 2 counts, and have at
            #                 least 20% of the counts of the most popular content warning tag
            #                 for the book.
            # - description: The book's blurb description from Hardcover.
            # - languages: An array of the book's languages, in ISO 639-3 code, from the
            #              languages of the book's editions in Hardcover's database.
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
            
            # TABLE populate_books_progress: Tracks the progress of populating the books table
            #                                for each genre. Used by the populateDatabase function
            #                                to know where to resume fetching books between runs.
            #  Columns:
            # - genre: The genre being populated. Primary key.
            # - last_page_fetched: The last page number that was fetched from the
            #                      Hardcover search API
            # - updated_at: Timestamp of the last update to this record. Automatically set
            #               to the current time whenever the record is updated.
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
            
            # TABLE book_embeddings: Stores embeddings for each book in the books table.
            #                        Used by the embedding generation process to avoid
            #                        re-generating embeddings for books that have already
            #                        been embedded.
            #  Columns:
            # - book_id: The ID of the book this embedding belongs to. Primary key and
            #            foreign key to books table.
            # - embedding: The embedding vector for the book.
            # - model_used: The name of the model used to generate the embedding.
            #               This lets us know which books to update if we change our
            #               embedding model.
            # - updated_at: Timestamp of the last update to this record. Automatically set
            #               to the current time whenever the record is updated.
            #
            # Note: Vector length has to be hardcoded, thanks to psycopg's injection
            # protections. Make sure this matches the dimensions of the embedding model
            # we're using. If we're using a Matryoshka model, we can trim the least
            # significant dimensions to fit the limit.
            cur.execute("""
                CREATE TABLE book_embeddings (
                    book_id BIGINT PRIMARY KEY REFERENCES books(id) ON DELETE CASCADE,
                    embedding VECTOR(768), 
                    model_used TEXT NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """) 
            cur.execute("""
                CREATE TRIGGER set_timestamp
                BEFORE UPDATE ON book_embeddings
                FOR EACH ROW
                EXECUTE FUNCTION trigger_set_timestamp()
            """)

            # TABLE inverted_index: Stores an mapping from keywords to books containing
            #                       those keywords, to support keyword search. Keywords
            #                       are split between title, author, description, and
            #                       each of the three tag groups (genre, mood, content)
            #                       to allow context-specific search. (For example, 
            #                       searching for books with "king" in the title should
            #                       not fetch books by Stephen King.)
            # Columns:
            # - keyword: The keyword being indexed. Primary key.
            # - title: An array of book IDs with titles containing this keyword.
            # - authors: An array of book IDs with authors containing this
            #                    keyword.
            # - description: An array of book IDs with descriptions containing 
            #                         this keyword.
            # - genre_tags: An array of book IDs with genre tags containing this
            #                       keyword.
            # - mood_tags: An array of book IDs with mood tags containing this 
            #                      keyword.
            # - content_tags: An array of book IDs with content warning tags
            #                         containing this keyword
            # - updated_at: Timestamp of the last update to this record. Automatically
            #               set to the current time whenever the record is updated.
            cur.execute("""
                CREATE TABLE inverted_index (
                    keyword TEXT PRIMARY KEY,
                    title BIGINT[],
                    authors BIGINT[],
                    description BIGINT[],
                    genre_tags BIGINT[],
                    mood_tags BIGINT[],
                    content_tags BIGINT[],
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.execute(""" 
                CREATE TRIGGER set_timestamp
                BEFORE UPDATE ON inverted_index
                FOR EACH ROW
                EXECUTE FUNCTION trigger_set_timestamp()
            """)
            conn.commit()
    

def addBooksToDatabase(response: dict, ignore_last_updated: bool = False) -> None:
  
    if "data" not in response or "books" not in response["data"]:
        print(f"Unexpected API response format. Response: {response}")
        raise JSONDecodeError("Unexpected API response format", str(response), 0)

    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor() as cur:
            cur = conn.cursor(row_factory=rows.dict_row)
            for book in response["data"]["books"]:
                if not book or type(book) != dict:
                    print("Unexpected book format in API response. Skipping.")
                    print(f"Book: {book}")
                    continue
                hardcover_id = book["id"]
                book["hardcover_id"] = hardcover_id # Add the hardcover ID to the book
                                                    #dict, so we don't lose it when we
                                                    # overwrite id with the database ID.

                # Fetch existing book data from the database, if it exists, to check if
                # we need to update the record. The old entry is also need to update the
                # inverted index if the book's metadata has changed significantly.
                print(f"{hardcover_id=}")
                cur.execute(sql.SQL("SELECT * FROM books WHERE hardcover_id = %s"), (hardcover_id,))
                existing_book = cur.fetchone()
                if existing_book and not ignore_last_updated:
                    # If the book was updated less than a week ago, we can skip it to
                    # save API calls and database writes, since it's unlikely to have 
                    # changed much in that time.
                    existing_updated_at = existing_book["updated_at"]
                    if existing_updated_at and (datetime.now(timezone.utc) - existing_updated_at).days < 7:
                        print(f'Book "{existing_book["title"]}" ({hardcover_id}) already exists in the database and is up to date. Skipping.')
                        continue

                # Extract relevant fields from the API response
                title = book["title"]
                description = book["description"]
                average_rating = book["rating"]

                if not book["contributions"]:
                    print(f'Book "{title}" ({hardcover_id}) has no contributors data. Skipping.')
                    continue
                authors: list[str] = []
                for contributor in book["contributions"]:
                    if contributor["contribution"] in ["Author", "Editor", None]:
                        authors.append(contributor["author"]["name"])
                book["authors"] = authors

                num_good_ratings = sum([rating["count"] for rating in book["ratings_distribution"] if rating["rating"] >= 3.5])

                if not book["editions"]:
                    print(f'Book "{title}" ({hardcover_id}) has no editions. Skipping.')
                    continue
                first_edition = book["editions"][0]
                isbn_13 = re.sub(r"[^0-9]", "", first_edition["isbn_13"])
                release_date = book["release_date"]
                if "BC" in release_date:    # psycopg freaks out if the release date is
                                            # before 1 CE. Sorry, Homer.
                    release_date = "0001-01-01"
                language_set: set[str] = set()
                for edition in book["editions"]:
                    if edition != None and edition["language"] != None and edition["language"]["code3"] != None:
                        language_set.add(edition["language"]["code3"])
                languages = list(language_set)

                tags = book["cached_tags"] 
                highest_genre_tag_count = max([tag["count"] for tag in tags["Genre"]], default=0)
                genre_tags = [tag["tag"] for tag in tags["Genre"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_genre_tag_count]
                book["genre_tags"] = genre_tags
                highest_mood_tag_count = max([tag["count"] for tag in tags["Mood"]], default=0)
                mood_tags = [tag["tag"] for tag in tags["Mood"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_mood_tag_count]
                book["mood_tags"] = mood_tags
                highest_content_tag_count = max([tag["count"] for tag in tags["Content Warning"]], default=0)
                content_tags = [tag["tag"] for tag in tags["Content Warning"] if tag["count"] >= 2 and tag["count"] >= 0.2 * highest_content_tag_count]
                book["content_tags"] = content_tags

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

                book_id_response = cur.execute(sql.SQL("SELECT id FROM books WHERE hardcover_id = %s"), (hardcover_id,)).fetchone()
                if not book_id_response or "id" not in book_id_response:
                    print(f"Failed to retrieve book ID for Hardcover ID {hardcover_id} after insert/update.")
                    raise Exception(f"Failed to retrieve book ID for Hardcover ID {hardcover_id} after insert/update.")
                book["id"] = book_id_response["id"]

                # Create or update the book embedding
                book_summary_string = (
                    f"search_document: Title: {title}\n"
                    + f"Authors: {', '.join(authors)}\n"
                    + f"Genre tags: {', '.join(genre_tags)}\n"
                    + f"Mood tags: {', '.join(mood_tags)}\n"
                    + f"Content tags: {', '.join(content_tags)}\n"
                    + f"Description:\n{description}"
                )
                embedding_vector = embeddings.createEmbedding(book_summary_string)
                cur.execute(sql.SQL("""
                    INSERT INTO book_embeddings (book_id, embedding, model_used)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (book_id) DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        model_used = EXCLUDED.model_used
                """), (
                    book["id"],
                    embedding_vector,
                    loadenv.loadEnvVariable("EMBEDDING_MODEL")
                ))

                # Update the inverted index for this book
                updateInvertedIndex(cur, book, existing_book)
            conn.commit()


def updateInvertedIndex(cur, new_book: dict, existing_book: dict | None) -> None:
    fields = ["title", "authors", "description", "genre_tags", "mood_tags", "content_tags"]
    stemmer = PorterStemmer()

    tokens_in_new_book: dict[str, list[str]] = {}

    for field in fields:
        field_items = new_book[field]
        if isinstance(field_items, list):
            tokens_in_new_book[field] = []
            for item in field_items:
                if item:
                    # In lists, tokens should not be split by whitespace, since they
                    # often represent distinct tags or entities. For example, the
                    # genre tag "Science Fiction" should be treated as a single 
                    # keyword, not split into "Science" and "Fiction". We also want
                    # to preserve punctuation. We still want to lowercase and 
                    # strip surrounding whitespace for consistency.
                    keyword = item.lower().strip()
                    tokens_in_new_book[field].append(keyword)
        elif isinstance(field_items, str):
            # For strings, we want to split into individual keywords by whitespace,
            # since they often contain multiple words that should be searchable
            # seperately. We also want to remove stop words and punctuation, then
            # stem the remaining words.
            keywords = re.findall(r"\w+", field_items.lower())
            tokens_in_new_book[field] = [stemmer.stem(keyword) for keyword in keywords if keyword not in STOP_WORDS]
        else:   
            # If it's not a list or a string, then the field is probably None.
            tokens_in_new_book[field] = []

    if not existing_book:
        tokens_in_old_book = {field: [] for field in fields}
    else:
        tokens_in_old_book: dict[str, list[str]] = {}
        for field in fields:
            field_items = existing_book[field]
            if isinstance(field_items, list):
                tokens_in_old_book[field] = []
                for item in field_items:
                    if item:
                        keyword = item.lower().strip()
                        tokens_in_old_book[field].append(keyword)
            elif isinstance(field_items, str):
                keywords = re.findall(r"\w+", field_items.lower())
                tokens_in_old_book[field] = [stemmer.stem(keyword) for keyword in keywords if keyword not in STOP_WORDS]

    tokens_in_old_book_not_in_new_book: dict[str, set[str]] = {}
    tokens_in_new_book_not_in_old_book: dict[str, set[str]] = {}
    for field in fields:
        tokens_in_old_book_not_in_new_book[field] = set([token for token in tokens_in_old_book[field] if token not in tokens_in_new_book[field]])
        tokens_in_new_book_not_in_old_book[field] = set([token for token in tokens_in_new_book[field] if token not in tokens_in_old_book[field]])

    for field in fields:
        for token in tokens_in_old_book_not_in_new_book[field]:
            cur.execute(sql.SQL("""
                UPDATE inverted_index SET {field} = array_remove({field}, %s)
                WHERE keyword = %s
            """).format(field=sql.Identifier(field)), (new_book["id"], token))
        for token in tokens_in_new_book_not_in_old_book[field]:
            cur.execute(sql.SQL("""
                INSERT INTO inverted_index (keyword, {field})
                VALUES (%s, ARRAY[%s]::BIGINT[])
                ON CONFLICT (keyword) DO UPDATE SET
                    {field} = array_append(inverted_index.{field}, %s)
            """).format(field=sql.Identifier(field)), (token, new_book["id"], new_book["id"]))


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

    for genre in genres_to_search:
        if db_size >= max_books_db_size:
            print(f"Database has reached the maximum size of {max_books_db_size} books. Stopping population.")
            break

        page: int
        with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
            with conn.cursor() as cur:    
                query = sql.SQL("SELECT last_page_fetched FROM populate_books_progress WHERE genre = %s").format(sql.Identifier(genre))
                cur.execute(query, (genre,))
                result = cur.fetchone()
                page = int(result[0] if result else 0)

        # Repeat until we add new books to the database, the response is empty, or
        # the API returns an error.
        for tries in range(max_repeated_pages):

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
                        where: {literary_type_id: {_eq: 1}, id: {_in: %s}, editions_count: {_gt: 0}}
                    ) {
                        id
                        title
                        contributions {
                            contribution
                            author {
                                name                            
                            }
                        }
                        release_date
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
            
            print(f"No new books added for genre {genre} on page {page}. Repeated books updated.")
            if tries < max_repeated_pages - 1: # Don't print this message on the last try, since we'll be moving to the next genre anyway
                print("Trying the next page.")
            else:
                print(f"Reached the maximum number of tries ({max_repeated_pages}) without adding new books. Moving to the next genre.")
            sleep(2) # Keep being nice to the API. (They've been so nice to us!)

        with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
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


def book_revision_test() -> None:
    reset_database() # Clear the database to ensure a clean slate for testing book revisions.
    test_book = {
        "title": "Lift",
        "contributions": [
            {
                "contribution": "Author",
                "author": {
                    "name": "Patrick Reding"
                }
            }
        ],
        "description": "A bird earns the chance to fly competitively on a prestigious university team and must prove to himself and his suspicious teammates that he belongs there, or lose his scholarship and boyfriend.",
        "cached_tags": {
            "Genre": [
                {"tag": "Young Adult", "count": 5000},
                {"tag": "Sports", "count": 4000},
                {"tag": "LGBTQ+", "count": 3000},
                {"tag": "Contemporary Fantasy", "count": 2000},
                {"tag": "Fantasy", "count": 1000},
                {"tag": "Fiction", "count": 500},
                {"tag": "Furry Fiction", "count": 400},
            ],
            "Mood": [   
                {"tag": "Uplifting", "count": 6000},
                {"tag": "Heartwarming", "count": 5000},
                {"tag": "Emotional", "count": 4000},
                {"tag": "Mysterious", "count": 1000},
                {"tag": "Dramatic", "count": 1000},
            ],
            "Content Warning": [
                {"tag": "Injury/Injury Detail", "count": 5000},
                {"tag": "Medical Content", "count": 4000},
                {"tag": "Bullying", "count": 3000},
                {"tag": "Betrayal", "count": 2000},
                {"tag": "Homophobia", "count": 1100},
            ]      
        },
        "genre_tags": ["Young Adult", "Sports", "LGBTQ+", "Contemporary Fantasy", "Fantasy"],
        "mood_tags": ["Uplifting", "Heartwarming", "Emotional"],
        "content_tags": ["Homophobia", "Injury/Injury Detail", "Medical Content", "Bullying", "Betrayal"],
        "id": 123456789,
        "editions": [
            {
                "release_date": "2028-01-01",
                "isbn_13": "9781250867398",
                "language": {"code3": "eng"}
            }
        ],
        "ratings_distribution": [
            {"rating": 5.0, "count": 8000},
            {"rating": 4.0, "count": 1001},
            {"rating": 3.0, "count": 500},
            {"rating": 2.0, "count": 300},
            {"rating": 1.0, "count": 199},
        ],
        "rating": 4.6303,
        "release_date": "2028-01-01",
    }
    test_book_revised = {
        "title": "Lift: Book 1 of The Aerobats",
        "contributions": [
            {
                "contribution": "Author",
                "author": {
                    "name": "Patrick D. Reding"
                }
            }
        ],
        "description": "A bird earns the chance to fly competitively on a prestigious university team and must prove to himself and his suspicious teammates that he belongs there, or lose his scholarship and boyfriend. This is the first book in the Aerobats series.",
        "cached_tags": {
            "Genre": [
                {"tag": "Young Adult", "count": 5000},
                {"tag": "Sports", "count": 4000},
                {"tag": "LGBTQ", "count": 3000},
                {"tag": "Contemporary Fantasy", "count": 2000},
                {"tag": "Fantasy", "count": 1200},
                {"tag": "Fiction", "count": 500},
                {"tag": "Furry Fiction", "count": 1200},
            ],
            "Mood": [   
                {"tag": "Uplifting", "count": 6000},
                {"tag": "Heartwarming", "count": 5000},
                {"tag": "Emotional", "count": 4000},
                {"tag": "Mysterious", "count": 3000},
                {"tag": "Dramatic", "count": 2000},
            ],
            "Content Warning": [
                {"tag": "Injury/Injury Detail", "count": 6000},
                {"tag": "Medical Content", "count": 4000},
                {"tag": "Bullying", "count": 3000},
                {"tag": "Betrayal", "count": 2000},
                {"tag": "Homophobia", "count": 1100},
            ]      
        },
        "id": 123456789,
        "editions": [
            {
                "release_date": "2028-02-14",
                "isbn_13": "9781250867398",
                "language": {"code3": "eng"}
            },
            {
                "release_date": "2028-07-07",
                "isbn_13": "9781250867399",
                "language": {"code3": "fra"}
            },
        ],
        "ratings_distribution": [
            {"rating": 5.0, "count": 8993},
            {"rating": 4.0, "count": 1007},
            {"rating": 3.0, "count": 500},
            {"rating": 2.0, "count": 300},
            {"rating": 1.0, "count": 200},
        ],
        "rating": 4.663,
        "release_date": "2028-02-14",
    }
    addBooksToDatabase({"data": {"books": [test_book]}})
    print("Added test book to database.")

    addBooksToDatabase({"data": {"books": [test_book_revised]}}, ignore_last_updated=True)
    print("Updated test book in database.")
    print("Check that the book's data was updated correctly, and that the inverted index was updated to reflect the changes in keywords.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape book data from the Hardcover API into the database.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    reset_parser = subparsers.add_parser("reset", help="Clear and reset the database. Use with caution!")

    populate_parser = subparsers.add_parser("populate", help="Fetch book data from the Hardcover API and add it to the database.")
    populate_parser.add_argument("--genres", nargs="+", help="Optional list of genres to populate. If not provided, will populate a default set of popular genres.")

    background_parser = subparsers.add_parser("background", help="Run the scraper in the background, periodically checking for new books to add. Not implemented yet.")
    background_parser.add_argument("--interval", type=int, default=30, help="Interval in minutes between checks for new books. Default is 30 minutes.")

    test_api_parser = subparsers.add_parser("test_api", help="Test the API connection and add some books to the database.")
    test_revision_parser = subparsers.add_parser("test_revision", help="Test updating a book's data in the database, and check that the inverted index is updated correctly.")

    args = parser.parse_args()
    match args.command:
        case "reset":
            reset_database() 
        case "populate":
            if args.genres:
                print(f"Populating database with books from the following genres: {', '.join(args.genres)}")
                populateDatabase(genres=args.genres)
            else:
                print(f"Populating database with canonical genres:")
                print("[Fantasy, Science Fiction, Romance, Thriller, Mystery, Young Adult, Horror, Juvenile Fiction, Literary, Classics]")
                populateDatabase()
        case "background":
            raise NotImplementedError("Background mode is not implemented yet.")
        case "test_api":   
            hardcover_api_test()
        case "test_revision":
            book_revision_test()
        case _:
            print("Invalid command.")
            exit(1)

if __name__ == "__main__":
    main()