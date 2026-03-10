import os
import sys
import psycopg
import argparse
import flask
import flask_cors
import json
import uuid
from datetime import date
from collections import defaultdict
from psycopg import sql, rows
from pgvector.psycopg import register_vector, Vector

import llm

# Add the workspace root to the path so imports work regardless of where the script is run from
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_python.src import loadenv
from shared_python.src import db_queries
from shared_python.src import embeddings

server = flask.Flask(__name__)
flask_cors.CORS(server)

def json_serial(obj) -> str:
    '''
    json_serial: JSON serializer for objects not serializable by default json code

    :param obj: The object to serialize.
    :type obj: Unknown
    :return: The serialized object, as a string.
    :rtype: str
    '''

    if isinstance(obj, date):
        return obj.isoformat()
    
    if isinstance(obj, uuid.UUID):
        return str(obj)

    raise TypeError(f"Type {type(obj)} not serializable")


@server.get('/')
def handleStatus() -> str:
    '''
    handleStatus: HTTP handler. A basic check that the engine API is online.

    :return: A simple message confirming the engine in online.
    :rtype: str
    '''
    return "Bookrover engine is <b>online</b>!"


@server.post('/reading_lists')
def handleGenerate() -> str:
    '''
    handleGenerate: HTTP handler. Service for generating new reading lists from a user 
                    prompt.

    :return: A JSON response with the ID of the new reading list.
    :rtype: str
    '''
    if not flask.request or not flask.request.is_json:
        flask.abort(400) # Bad request

    request = flask.request.get_json()
    if "query" not in request:
        flask.abort(400) # Bad request
    
    if "extend_list_id" in request:
        extend_list_id = str(request["extend_list_id"])
        limit = 5
        list = db_queries.fetchReadingListInfo(reading_list_id=extend_list_id, include_removed=True)
        if "error" in list:
            flask.abort(500)
        keywords = json.loads(list["keywords"])
        ids_to_exclude = [book["id"] for book in list["books"]]

    else:
        extend_list_id = None
        limit = 10
        ids_to_exclude = None
        keywords = None

    response = {"reading_list_id": generateReadingList(
        request["query"], limit=limit, ids_to_exclude = ids_to_exclude,
        keywords = keywords, extend_list_id=extend_list_id
    )}
    return json.dumps(response)


@server.get('/reading_lists')
def handleFetchList() -> str:
    '''
    handleFetchList: HTTP handler. Service for returning informaton about a reading list,
                     including contents and metadata.

    :return: A JSON response with the reading list information.
    :rtype: str
    '''
    if not flask.request:
        flask.abort(400) # Bad request

    reading_list_id: str | None = flask.request.args.get('reading_list_id')
    if not reading_list_id:
        response = fetchReadingLists()
    else:
        response = db_queries.fetchReadingListInfo(reading_list_id=reading_list_id)

    if "error" in response:
        if "not found" in response["error"]:
            flask.abort(404) # Not found
        else:
            flask.abort(500) # Internal server error

    return json.dumps(response, default=json_serial)


@server.put('/reading_lists/update_name')
def handleUpdateListName() -> str:
    '''
    handleUpdateListName: HTTP handler. Service for renaming reading lists.

    :return: Confirmation that the name has been updated.
    :rtype: str
    '''
    if not flask.request:
        flask.abort(400) # Bad request

    reading_list_id: str | None = flask.request.args.get('reading_list_id')
    name: str | None = flask.request.args.get('name')
    if (not reading_list_id) or (not name):
        flask.abort(400) # Bad request
    
    if updateListName(reading_list_id=reading_list_id, name=name):
        return "Name updated!"
    else:
        flask.abort(500)


@server.delete('/reading_lists')
def handleDeleteList() -> str:
    '''
    handleDeleteList: HTTP handler. Service for deleting reading lists.

    :return: Confirmation that the list has been deleted.
    :rtype: str
    '''
    if not flask.request:
        flask.abort(400) # Bad request

    reading_list_id: str | None = flask.request.args.get('reading_list_id')
    if not reading_list_id:
        flask.abort(400) # Bad request
    
    if deleteList(reading_list_id=reading_list_id):
        return "List deleted."
    else:
        flask.abort(500)


@server.delete('/reading_lists/book')
def handleDeleteListBook() -> str:
    '''
    handleDeleteListBook: HTTP handler. Service for removing a book from a reading list.

    :return: Confirmation that the book has been removed.
    :rtype: str
    '''
    if not flask.request:
        flask.abort(400) # Bad request

    reading_list_id: str | None = flask.request.args.get('reading_list_id')
    if not reading_list_id:
        flask.abort(400) # Bad request

    book_id: str | None = flask.request.args.get('book_id')
    if not book_id:
        flask.abort(400) # Bad request
    
    if deleteListBook(reading_list_id=reading_list_id, book_id=int(book_id)):
        return "Book removed from list."
    else:
        flask.abort(500)


def runAsServer() -> None:
    '''
    runAsServer: Starts the engine as an API server.

    :return: None
    :rtype: None
    '''
    port = int(loadenv.loadEnvVariable("ENGINE_PORT"))
    server.run(port=port)


def fetchReadingLists(user_id: int = 0) -> dict:
    '''
    fetchReadingLists: Returns the name and ID of each list in the reading_lists database
                       table created by the given user.

    :param user_id: The id of the user.
    :type user_id: int
    :return: A dictionary containing the name and id of each reading list.
    :rtype: dict
    '''
    try:
        with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
            with conn.cursor(row_factory=rows.dict_row) as cur:
                cur.execute(
                    sql.SQL("""
                        SELECT id, name, created_at FROM reading_lists
                            WHERE user_id = %s
                    """), (user_id,)
                )
                response = cur.fetchall()

        if not response:
            return {"lists": []}
        else:
            return {"lists": response}
        
    except psycopg.Error:
        return {"error": "Database error"}





def updateListName(reading_list_id: str, name: str) -> bool:
    '''
    updateListName: Changes the name of the given reading list in the database.

    :param reading_list_id: The id of the reading list, as a UUID.
    :type reading_list_id: str
    :param name: The new name for the reading list.
    :type name: str
    :return: Whether the operation succeeded.
    :rtype: bool
    '''
    try:
        with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
            with conn.cursor(row_factory=rows.dict_row) as cur:
                cur.execute(
                    sql.SQL("""
                        UPDATE reading_lists
                            SET name = %s
                            WHERE id = %s
                    """), (name, reading_list_id)
                )
                if cur.rowcount > 0:
                    conn.commit()
                    return True
                else:  
                    return False
    except psycopg.Error:
        return False


def deleteList(reading_list_id: str) -> bool:
    '''
    deleteList: Deletes the given reading list from the database.

    :param reading_list_id: The id of the reading list, as a UUID.
    :type reading_list_id: str
    :return: Whether the operation succeeded.
    :rtype: bool
    '''
    try:
        with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
            with conn.cursor(row_factory=rows.dict_row) as cur:
                cur.execute(
                    sql.SQL("""
                        DELETE FROM reading_lists
                            WHERE id = %s
                    """), (reading_list_id,)
                )
                if cur.rowcount > 0:
                    conn.commit()
                    return True
                else:  
                    return False
    except psycopg.Error:
        return False


def deleteListBook(reading_list_id: str, book_id: int) -> bool:
    '''
    deleteList: Removes the given book from the reading list.

    :param reading_list_id: The id of the reading list, as a UUID.
    :type reading_list_id: str
    :param book_id: The id of the book to remove.
    :type book_id: int
    :return: Whether the operation succeeded.
    :rtype: bool
    '''
    try:
        with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
            with conn.cursor(row_factory=rows.dict_row) as cur:
                cur.execute(
                    sql.SQL("""
                        UPDATE reading_list_books
                            SET removed = true
                            WHERE (reading_list_id = %s) AND (book_id = %s)
                    """), (reading_list_id, book_id)
                )
                if cur.rowcount > 0:
                    conn.commit()
                    return True
                else:  
                    return False
    except psycopg.Error:
        return False


def semanticSearch(query: str, limit: int = 10, ids_to_search: set[int] | None = None) -> list[int]:
    '''
    semanticSearch: Performs a semantic search for books matching the given query,
                    using the book embeddings table.
    
    :param query: The query string to search for. This will be converted into an embedding
                  and compared against the book embeddings in the database.
    :type query: str
    :param limit: The maximum number of results to return. Defaults to 10.
    :type limit: int
    :param ids_to_search: The set of valid ids to search, narrowed down by the keyword search.
                          If not provided, we search the entire database.
    :type ids_to_search: set[int] | None
    :return: A list of dictionaries, each containing a book's metadata.
             The list is sorted in descending order of similarity.
    :rtype: list[dict[str, Any]]
    '''

    #Embed the query
    # Note: We're keeping this as simple as possible for now, but we need to work out
    # what preprocessing steps would improve search results.
    query_embedding = embeddings.createEmbedding(f"search_query: {query.strip()}")
    # print(f"{query_embedding=}") # Debug: Print the query embedding to verify it's being created correctly.

    # Connect to the database and perform a cosine similarity search against the book
    # embeddings. We just need the ranking, not the score.
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        register_vector(conn) # Register the vector type with psycopg so it can be used in queries.
        with conn.cursor(row_factory=rows.dict_row) as cur:
            # Get the closest embeddings by cosine similarity (pgvector's "<=>" operator).

            if not ids_to_search:
                cur.execute(
                    sql.SQL("""
                        SELECT book_id
                        FROM book_embeddings
                        ORDER BY (embedding <=> %s) ASC
                        LIMIT %s;
                    """),
                    (Vector(query_embedding), limit)
                )
            else:
                cur.execute(
                    sql.SQL("""
                        SELECT book_id
                        FROM book_embeddings
                        WHERE book_id = ANY (%s)
                        ORDER BY (embedding <=> %s) ASC
                        LIMIT %s;
                    """),
                    (list(ids_to_search), Vector(query_embedding), limit)
                )
            ids = [int(row["book_id"]) for row in cur.fetchall()]

            # Fetch the matching book metadata from the books table, preserving the order
            # of the results.
            #results = []
            #for id in ids:
            #    cur.execute(
            #        sql.SQL("""
            #            SELECT *
            #            FROM books
            #            WHERE id = %s;
            #        """),
            #        (id,)
            #    )
            #    results.append(cur.fetchone())

    #return results
    return ids


def keywordSearch(query: str, limit: int = 10, ids_to_exclude: list[int] | None = None,
                  keywords: list[dict] | None = None) -> tuple[list[int], set[int], list[dict]]:
    '''
    semanticSearch: Performs a keyword search for books matching the given query,
                    using an LLM hook to extract and weight valid keywords from the query.
    
    :param query: The query string to search for. This will be converted into an embedding
                  and compared against the book embeddings in the database.
    :type query: str
    :param limit: The maximum number of results to return. Defaults to 10.
    :type limit: int
    :param ids_to_exclude: The set of ids to exclude from search, usually because they're
                           already in the reading list. If not provided, we search the entire
                           database.
    :type ids_to_exclude: list[int] | None
    :param keywords: A list of keyword dictionaries. If provided, we use this instead of the
                     LLM extraction. Important for extending reading lists.
    :type keywords: list[dict] | None
    :return: A tuple of:

                - A list of dictionaries, each containing a book's metadata, sorted in
                  descending order of similarity.

                - The set of remaining valid book IDs not excluded by mandatory or
                  disqualifying keywords.
                  
                - The list of keyword dictionaries.
    :rtype: tuple[list[int], set[int], list[dict]]
    '''
    positive_keyword_weight = 1.0
    negative_keyword_weight = 1.0

    if ids_to_exclude is None:
        ids_to_exclude = []

    # Extract the relevant keywords from the query
    if keywords is None:
        keywords = llm.extractKeywords(query)
    # print(keywords) # TEST

    disqualified_ids: set[int] = set(ids_to_exclude) # IDs matching any disqualifying tags
    mandatory_ids: set[int] = set(db_queries.fetchAllIds()) - disqualified_ids # IDs matching all mandatory tags
    positive_counts_by_id = defaultdict(int) # Number of positive hits for each ID
    negative_counts_by_id = defaultdict(int) # Number of negative hits for each ID

    for keyword in keywords:
        for category in keyword["categories"]:
            ids = db_queries.fetchIdsFromII(keyword["keyword"], category)
            match keyword["strength"]:
                case "mandatory":
                    mandatory_ids = mandatory_ids.intersection(ids)

                case "disqualifying":
                    disqualified_ids = disqualified_ids.union(disqualified_ids)
                    mandatory_ids -= disqualified_ids

                case "positive":
                    for id in ids:
                        positive_counts_by_id[id] += 1
                
                case "negative":
                    for id in ids:
                        negative_counts_by_id[id] += 1
    
    # Note: If we have eliminated all possible books with hard tags, mandatory_ids will
    #       be empty, the same as if we had no mandatory tags. In this case, we 
    #       automagically ignore all mandatory tags while preserving disqualifying tags,
    #       which is a decent fallback behaviour.
    if not mandatory_ids:
        mandatory_ids = set(db_queries.fetchAllIds()) - disqualified_ids

    # If the list is still empty after this, then return the empty list
    if not mandatory_ids:
        return [], set(), keywords

    # We use a geometric progression for scoring soft keywords: The first positive hit
    # is worth 1 point, the second 1/2, the third 1/4, and so on. This reduces the impact
    # of subsequent matches, even with large numbers of keywords to match. This also
    # keep a small finite bound on scores, since the series converges to 2.

    scores = defaultdict(float)

    for key in positive_counts_by_id:
        if key in mandatory_ids:
            scores[key] = positive_keyword_weight * (2.0 - 0.5**(positive_counts_by_id[key] - 1))
        else:
            scores[key] = float("-inf")
    for key in negative_counts_by_id:
        if key in mandatory_ids:
            scores[key] -= negative_keyword_weight * (2.0 - 0.5**(negative_counts_by_id[key] - 1))
        else: 
            scores[key] = float("-inf")
    for key in disqualified_ids:
        scores[key] = float("-inf")
    
    result = sorted(mandatory_ids, key = lambda x: scores[x], reverse = True)
    return (result[:limit], mandatory_ids, keywords)


def generateReadingList(user_prompt: str, limit: int = 10,
                        ids_to_exclude: list[int] | None = None,
                        keywords: list[dict] | None = None,
                        extend_list_id: str | None = None) -> str:
    '''
    generateReadingList: Fetches the top ten books matching the user's prompt to generate a 
                         reading list, or the top five to extend an existing list.
    
    :param user_prompt: The user prompt
    :type user_prompt: str    
    :param book_ids_to_exclude: Book ids to exclude from the search, typically becuase
                                they have already been read, are already in the reading
                                list, or have been flagged as disliked.
    :type book_ids_to_exclude: list[int] | None
    :param keywords: A list of keyword dictionaries. If provided, we use this instead of
                     LLM extraction for keyword search. Important for extending reading lists.
    :param extend_list_id: The UUID of the reading list we're extending, if any. If not
                           provided, we create a new list.
    :type keywords: list[dict] | None
    :return: The UUID of the new or extended reading list.
    :rtype: str
    '''

    user_id = 0

    semantic_weight = 0.48
    keyword_weight = 0.32
    popularity_weight = 0.10
    recency_weight = 0.10
    # DEV NOTE: We will eventually add user preferences a a factor

    limit_after_keyword = limit * 20
    limit_after_semantic = limit * 10
    limit_after_total_score = limit * 2

    rrf_k_score = 60

    # Step 1: Run a keyword search first to eliminate books that fail hard conditions.
    #         Use the ranking to generate a Reciprocal Rank Fusion score for keyword
    #         search.
    book_ids, ids_to_search, keywords = keywordSearch(user_prompt, limit_after_keyword,
                                            ids_to_exclude, keywords)
    if len(ids_to_search) == 0:
        print("WARNING: Keyword search eliminated all books in database.")
        print("         Falling back to semantic search.")
        ids_to_search = None
        semantic_weight += keyword_weight
        keyword_weight = 0.0

    scores = defaultdict(dict)
    for rank, id in enumerate(book_ids, start=1):
        scores[id]["keyword"] = 1 / (rrf_k_score + rank)

    # Step 2: Run a semantic search on the remaining books, again using RRF to get a
    #         score.

    book_ids = semanticSearch(user_prompt, limit_after_semantic, set(book_ids))
    for rank, id in enumerate(book_ids, start=1):
        scores[id]["semantic"] = 1 / (rrf_k_score + rank)

    # Filter out entries that don't have a semantic score
    scores = {id: score_dict for id, score_dict in scores.items() if "semantic" in score_dict}

    # Step 3: Add popularity and recency RRF scores
    id_to_popularity: list[tuple[int, int]] = []
    id_to_release_date: list[tuple[int, date]] = []
    for id in scores:
        book = db_queries.fetchBookByID(id)
        if book:
            id_to_popularity.append((id, book["num_good_ratings"]))
            id_to_release_date.append((id, book["release_date"]))

    id_to_popularity.sort(key=lambda x: x[1], reverse=True)
    id_to_release_date.sort(key=lambda x: x[1], reverse=True)
    
    for rank, id_tuple in enumerate(id_to_popularity, start=1):
        scores[id_tuple[0]]["popularity"] = 1 / (rrf_k_score + rank)
    for rank, id_tuple in enumerate(id_to_release_date, start=1):
        scores[id_tuple[0]]["recency"] = 1 / (rrf_k_score + rank)

    # Step 4: Calculate total scores and ranks
    for id in scores:
        scores[id]["total"] = 0
        if keyword_weight > 0 and "keyword" in scores[id]:
            scores[id]["total"] += keyword_weight * scores[id]["keyword"] 
        if semantic_weight > 0 and "semantic" in scores[id]:
            scores[id]["total"] += semantic_weight * scores[id]["semantic"] 
        if popularity_weight > 0 and "popularity" in scores[id]:
            scores[id]["total"] += popularity_weight * scores[id]["popularity"]
        if recency_weight > 0 and "recency" in scores[id]:
            scores[id]["total"] += recency_weight * scores[id]["recency"]

    book_ids.sort(key = lambda id: scores[id]["total"], reverse=True)

    print("Book ranks by category:")
    for index in range(min(limit_after_total_score, len(book_ids))):
        id = book_ids[index]
        print(f"Rank: {index+1}, id: {id}")
        for key in scores[id]:
            if key == "total":
                print(f"{key}: {scores[id][key]:.02f}", end="\t")
            else:
                print(f"{key}: {1/(scores[id][key]) - rrf_k_score:.0f}", end="\t")
        print()
    print()

    # Step 6: Save the results to the database
    with psycopg.connect(loadenv.getDatabaseConnectionString()) as conn:
        with conn.cursor() as cur:
            if extend_list_id is None:
                cur.execute(sql.SQL("""
                    INSERT INTO reading_lists (user_id, prompt, keywords)
                    VALUES (%s, %s, %s)                    
                    """), (user_id, user_prompt, json.dumps(keywords)))
                cur.execute(sql.SQL("""
                    SELECT id 
                        FROM reading_lists 
                        WHERE user_id = %s
                        ORDER BY updated_at DESC
                        LIMIT 1     
                    """), (user_id,))
                
                reading_list_id_row = cur.fetchone()
                start = 1
                if reading_list_id_row:
                    reading_list_id: str = str(reading_list_id_row[0])
                else:
                    raise Exception("Failed to create reading list in database.")
            else:
                reading_list_id = extend_list_id
                cur.execute(sql.SQL("""
                SELECT rank
                    FROM reading_list_books
                    WHERE reading_list_id = %s
                    ORDER BY rank DESC
                    LIMIT 1     
                """), (reading_list_id,))
                max_rank_row = cur.fetchone()
                if max_rank_row:
                    start = 1 + max_rank_row[0]
                else: # Somehow we have a reading list without books, so start from 1.
                    start = 1


            for rank, id in enumerate(book_ids[:limit], start=start):
                cur.execute(sql.SQL("""
                    INSERT INTO reading_list_books (reading_list_id, book_id, rank)
                    VALUES (%s, %s, %s)                    
                    """), (reading_list_id, id, rank))
            conn.commit()

    # Step 7: Return the reading list ID
    return reading_list_id


def main() -> None:
    '''
    main: The main entry point of the engine API.
    
    :return: None
    :rtype: None
    '''
    
    parser = argparse.ArgumentParser(description="Search engine for creating reading lists using hybrid keyword/semantic search.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="Generate a reading list from the provided query on the command line.")
    generate_parser.add_argument("query", help="The user's query")

    server_parser = subparsers.add_parser("server", help=f"Start the engine as an HTTP server on  port {loadenv.loadEnvVariable("ENGINE_PORT")}.")

    args = parser.parse_args()

    match args.command:    
        case "generate":
            query = args.query

            if not query:
                print("Please provide a search query.")
                return
            
            reading_list_id = generateReadingList(query)
            print(f"{reading_list_id=}")

        case "server":
            runAsServer()

if __name__ == "__main__":
    main()