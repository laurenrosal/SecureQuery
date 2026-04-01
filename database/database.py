import sqlite3
from pathlib import Path

#default location of the database inside the project 
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "securequery.db"

#Creating the connection for the database to SQLite 
def getconnection(db_path: str = None):
    #using the path or fall back to the defualt project DB
    database_path = db_path or DEFAULT_DB_PATH
    #connecting to the SQLite file
    connnection = sqlite3.connect(database_path)
    #Make the rows behave dicts - access columns by name, not index
    connnection.row_factory = sqlite3.Row
    return connnection