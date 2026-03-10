import psycopg
from psycopg import sql, rows
from nltk.stem import PorterStemmer

from . import loadenv

def getBookCount() -> int:
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM books")
            result = cur.fetchone()
            return result[0] if result else 0

def fetchBookByID(id: str | int) -> dict | None:  
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT * FROM books WHERE id = %s")
            cur.execute(query, (id,))
            result = cur.fetchone()
            if result and cur.description:
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, result))
            return None

def fetchBookByHardcoverID(hardcover_id: str | int) -> dict | None:  
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT * FROM books WHERE hardcover_id = %s")
            cur.execute(query, (hardcover_id,))
            result = cur.fetchone()
            if result and cur.description:
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, result))
            return None
        
def fetchIdsFromII(keyword: str, field: str) -> list[int]:
    keyword = keyword.lower()
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor() as cur:
            match field:
                case "title":
                    stemmer = PorterStemmer()
                    keyword = stemmer.stem(keyword)
                    query = sql.SQL("SELECT title FROM inverted_index WHERE keyword LIKE %s")

                case "authors" | "author":
                    query = sql.SQL("SELECT authors FROM inverted_index WHERE keyword LIKE %s")
                
                case "description":
                    stemmer = PorterStemmer()
                    keyword = stemmer.stem(keyword)
                    query = sql.SQL("SELECT description FROM inverted_index WHERE keyword LIKE %s")
                
                case "genre" | "genres" | "genre_tags":
                    query = sql.SQL("SELECT genre_tags FROM inverted_index WHERE keyword LIKE %s")

                case "mood" | "moods" | "mood_tags":
                    query = sql.SQL("SELECT genre_tags FROM inverted_index WHERE keyword LIKE %s")

                case "content" | "content warnings" | "content warning" | "content_tags":
                    query = sql.SQL("SELECT content_tags FROM inverted_index WHERE keyword LIKE %s")

                case _:
                    print("WARNING (fetchIdsFromIIByField): Unknown category")
                    return []

            cur.execute(query, ('%' + keyword + '%',))
            results = cur.fetchall()

    output_set = set()
    for result_row in results:
        result = result_row[0]
        if result:
            output_set = output_set.union(set(result))
    
    return list(output_set)

def fetchAllIds() -> list[int]:
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id from books")
            results = cur.fetchall()

    return [item[0] for item in results]

def fetchReadingListInfo(reading_list_id: str, include_removed: bool = False) -> dict:
    print(reading_list_id)
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor(row_factory=rows.dict_row) as cur:
            cur.execute(
                sql.SQL("""
                    SELECT user_id, name, prompt, keywords, created_at FROM reading_lists
                        WHERE id = %s
                """), (reading_list_id,)
            )
            response = cur.fetchone()
            if not response:
                return {"error": "Reading list not found."}
            response["books"] = []

            if include_removed:
                cur.execute(
                    sql.SQL("""
                        SELECT book_id FROM reading_list_books
                            WHERE reading_list_id = %s
                            ORDER BY rank ASC
                    """), (reading_list_id,)
                )
            else:
                cur.execute(
                    sql.SQL("""
                        SELECT book_id FROM reading_list_books
                            WHERE reading_list_id = %s AND NOT removed
                            ORDER BY rank ASC
                    """), (reading_list_id,)
                )
            book_ids = cur.fetchall()
            if not book_ids:
                return {"error": "Could not fetch books from reading list."}
            for book_id in book_ids:
                book = fetchBookByID(book_id["book_id"])
                response["books"].append(book)

    return response