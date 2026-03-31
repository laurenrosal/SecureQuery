import sqlite3
import pandas as pas
from database.database import getconnection

#maps pandas data types to SQLite data types
TYPE_MAP = {
    "int64": "INTEGER",
    "float64": "REAL",
    "bool": "INTEGER",
    "datetime64[ns]": "TEXT",
    "object": "TEXT",
}

class SchemaManager:
    def __init__(self, db_path=None):
        self.db_path = db_path

    def _get_conn(self):
        return getconnection(self.db_path)
    
    def get_tables(self):
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        return [r["name"] for r in rows]
    
    def get_table_schema(self, table_name):
        with self._get_conn() as conn:
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        schema = []
        for row in rows:
            schema.append({
                "name": row["name"],
                "type": row["type"]
            })
        return schema
        
    def get_all_schemas(self):
        all_schemas ={}

        for table in self.get_tables():
            all_schemas[table] = self.get_table_schema(table)

        return all_schemas
        
    def infere_sql_type(self, dtype):
        return TYPE_MAP.get(str(dtype), "TEXT")
    
    def table_exists(self, table_name):
        return table_name in self.get_tables()
    
    def schemas_match(self, table_name, df: pas.DataFrame):
        current_schema = {}
        for column in self.get_table_schema(table_name):
            if column["name"].lower() != "id":
                current_schema[column["name"].lower()] = column["type"]
        
        new_schema = {}
        for column_name, dtype in df.dtypes.items():
            new_schema[column_name.lower()] = self.infere_sql_type(dtype)
        
        return current_schema == new_schema
    
    def create_table(self, table_name, df: pas.DataFrame, conn: sqlite3.Connection):
        columns = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

        for column_name, dtype in df.dtypes.items():
            sql_type = self.infere_sql_type(dtype)
            columns.append(f'"{column_name}" {sql_type}')

        create_query = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
        conn.execute(create_query)