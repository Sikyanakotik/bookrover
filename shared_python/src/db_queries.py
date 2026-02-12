import psycopg
from psycopg import sql

from . import loadenv

def getBookCount() -> int:
    username = loadenv.loadEnvVariable("POSTGRES_USERNAME")
    password = loadenv.loadEnvVariable("POSTGRES_PASSWORD")
    host = loadenv.loadEnvVariable("POSTGRES_HOST")
    port = loadenv.loadEnvVariable("POSTGRES_PORT")
    
    with psycopg.connect(f'user={username} password={password} host={host} port={port} dbname=bookrover') as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM books")
            result = cur.fetchone()
            return result[0] if result else 0

def fetchBookByHardcoverID(hardcover_id: str | int) -> dict | None:
    username = loadenv.loadEnvVariable("POSTGRES_USERNAME")
    password = loadenv.loadEnvVariable("POSTGRES_PASSWORD")
    host = loadenv.loadEnvVariable("POSTGRES_HOST")
    port = loadenv.loadEnvVariable("POSTGRES_PORT")
    
    with psycopg.connect(f'user={username} password={password} host={host} port={port} dbname=bookrover') as conn:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT * FROM books WHERE hardcover_id = %s")
            cur.execute(query, (hardcover_id,))
            result = cur.fetchone()
            if result and cur.description:
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, result))
            return None