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

#This is responsible for understandinf and managing the structure of the database
class SchemaManager:
    def __init__(self, db_path=None):
        #store the path so every methnod can open its own connection
        self.db_path = db_path

    def _get_conn(self):
        #Internal helper - opens and returns a database connection
        return getconnection(self.db_path)
    
    def get_tables(self):
        #Returns a list of all tables names currently in the database
        #Queries sqlite_master which is interal registry of all objects
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        return [r["name"] for r in rows]
    

    def get_table_schema(self, table_name):
        #Returning the column information for a specific table
        #This using PRAGMA table_info() which returns one row per column
        with self._get_conn() as conn:
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
       
        #Extreact only name and type from each column metadata
        schema = []
        for row in rows:
            schema.append({
                "name": row["name"],
                "type": row["type"]
            })
        return schema
        
    def get_all_schemas(self):
        #Returns the schema for eveery table in the databse
        #This will help the method call by QueryService and LLMAdapter to 
        #get a full picutre of the database dtructure for building the SQL queries and prompts
        all_schemas ={}

        for table in self.get_tables():
            all_schemas[table] = self.get_table_schema(table)

        return all_schemas

    #convert a pandas dtype to the equivlent SQLite type string    
    def infere_sql_type(self, dtype):
        return TYPE_MAP.get(str(dtype), "TEXT")
    
    #check whether the table already exists in the database
    def table_exists(self, table_name):
        return table_name in self.get_tables()
    
    ##check wether a Datafram column match an exisiting table's schema
    def schemas_match(self, table_name, df: pas.DataFrame):
        #build a dict of the exsting table's columns (excluding 'id')
        current_schema = {}
        for column in self.get_table_schema(table_name):
            if column["name"].lower() != "id":
                current_schema[column["name"].lower()] = column["type"]
        
        #build a dict of the incoming dataframe'columns
        new_schema = {}
        for column_name, dtype in df.dtypes.items():
            new_schema[column_name.lower()] = self.infere_sql_type(dtype)
        
        return current_schema == new_schema
    
    #Generate and execute a Create table statement for a dataframe.
    def create_table(self, table_name, df: pas.DataFrame, conn: sqlite3.Connection):
        #Start with the auto-incrementing primary key every table needs 
        columns = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

        #add one column definition per Dataframe column
        for column_name, dtype in df.dtypes.items():
            sql_type = self.infere_sql_type(dtype)
            columns.append(f'"{column_name}" {sql_type}')
        
        #build and execute the full create table statement
        create_query = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
        conn.execute(create_query)