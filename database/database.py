import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "securequery.db"

def getconnection(db_path: str = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn