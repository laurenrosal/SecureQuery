import os
import sqlite3
import pytest
from database.database import getconnection

#run these test python3 -m pytest tests/ -v

def test_return_is_created(tmp_path):
    db_path = tmp_path / "test.db"
    conn = getconnection(str(db_path))

    #check atcully get a sqlite connection back
    assert isinstance(conn, sqlite3.Connection)

    conn.close()

def test_can_access_columns_by_name(tmp_path):
    db_path = str(tmp_path / "test.db")

    with getconnection(db_path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'hello')")

        result = conn.execute("SELECT * FROM test_table").fetchone()
    #row_factor should let us used column names      
    assert result["name"] == "hello"

def test_databse_file_gets_created(tmp_path):
    db_path = str(tmp_path / "new.db")

    #file shouldn't exist yet
    assert not os.path.exists(db_path)

    getconnection(db_path).close()

    #after conncting, sqlite should create the file
    assert os.path.exists(db_path)
