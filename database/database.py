import sqlite3
from pathlib import Path

#default location of the database inside the project 
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "securequery.db"

def getconnection(db_path: str = None):
    database_path = db_path or DEFAULT_DB_PATH
    connnection = sqlite3.connect(database_path)
    connnection.row_factory = sqlite3.Row
    return connnection