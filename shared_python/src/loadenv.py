'''
loadenv.py: Holds utility functions for managing environment variables, 
so we don't have to duplicate this code in multiple places.
'''

import os
from dotenv import load_dotenv
load_dotenv()

def loadEnvVariable(name: str) -> str:
    value = os.getenv(name)
    if value == None:
        raise EnvironmentError(f"{name} not found in environment.")
    return value

def getDatabaseConnectionString() -> str:
    username = loadEnvVariable("POSTGRES_USERNAME")
    password = loadEnvVariable("POSTGRES_PASSWORD")
    host = loadEnvVariable("POSTGRES_HOST")
    port = loadEnvVariable("POSTGRES_PORT")
    return f'user={username} password={password} host={host} port={port} dbname=bookrover'