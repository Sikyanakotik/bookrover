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