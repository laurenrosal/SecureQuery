import pandas as pd
import sqlite3
from database.database import getconnection
from database.schema_manager import SchemaManager

##loading any CSV file into a SQLite table
class CSVLoader:
    def __init__(self, db_path=None):
        #path to the sqlite database file
        self.db_path = db_path

        #schema manager handles all table structure decisions 
        #CSVLoader delegates schema work to it rather than doing it directly
        self.schema_manager = SchemaManager(db_path)

    def load(self, csv_path: str, table_name: str, on_conflict: str = "append") -> dict:
        #Read the CSV into a DataFrame
        df = pd.read_csv(csv_path)

        # Normalize column names so they work sefely as SQL identifiers
        df.columns = [
            c.strip().lower().replace(" ", "_") for c in df.columns
        ]

        with getconnection(self.db_path) as conn:
            if self.schema_manager.table_exists(table_name):
                #Tables already exists - decide what to do based on on_conflicts

                if on_conflict == "skip":
                    #Do nothing - return immediately without tounching the DB
                    return {"table": table_name, "rows_inserted": 0,
                            "action": "skipped"}
                

                elif on_conflict == "replace":
                    #remove all exisiting rows but keep the table structure 
                    #using DELETE instead of DROP TABKE preserves the schema
                    conn.execute(f'DELETE FROM "{table_name}"')
                    action = "replaced"

                
                elif on_conflict == "append":
                    #Only append if the CSV columms match the exisiting table
                    #Mismatched schemas would silently corrupt the data
                    if not self.schema_manager.schemas_match(table_name, df):
                        raise ValueError(
                            f"Schema mismatch: CSV columns do not match "
                            f"existing table '{table_name}'."
                        )
                    action = "appended"


                else:
                    #Guard against typos like on_conflict='overwrite'
                    raise ValueError(
                        f"Invalid on_conflict value: '{on_conflict}'. "
                        f"Must be 'append', 'replace', or 'skip'."
                    )
            else:
                #table doesn't exist yet - create it using the Dataframe's
                #column names and interred SQL types 
                self.schema_manager.create_table(table_name, df, conn)
                action = "created"

            # Insert all rows from the DataFrame into the table    
            rows_inserted = self._insert_rows(conn, table_name, df)

        return {"table": table_name, "rows_inserted": rows_inserted,
                "action": action}

    def _insert_rows(self, conn: sqlite3.Connection,
                     table_name: str, df: pd.DataFrame) -> int:
        
        # Insert every row from the DataFrame into the target table.
        # Builds a parameterized INSERT statement using '?' placeholders
        #to safely pass values — this prevents SQL injection.

        # Build the column list and matching placeholders for the INSERT
        cols = ", ".join(f'"{c}"' for c in df.columns)
        placeholders = ", ".join("?" for _ in df.columns)
        sql = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'

        count = 0
        # itertuples is faster than iterrows and returns plain tuples —
        # index=False skips the row index, name=None returns a plain tuple
        for row in df.itertuples(index=False, name=None):
            conn.execute(sql, row)
            count += 1
        return count