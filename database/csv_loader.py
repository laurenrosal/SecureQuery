import pandas as pas
import sqlite3
from database.database import getconnection
from database.schema_manager import SchemaManager

class CSVLoader:
    def __init__(self, db_path: str = None):
        self.db_path = db_path
        self.schema_manager = SchemaManager(db_path)

    def load(self, csv_path: str, table_name: str,
             on_conflict: str = "append") -> dict:
        
        """
        Load a CSV file into a SQLite table.

        """
        df = pas.read_csv(csv_path)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        with getconnection(self.db_path) as conn:
            if self.schema_manager.table_exists(table_name):
                if on_conflict == "skip":
                    return {"table": table_name, "rows_inserted": 0,
                            "action": "skipped"}
                elif on_conflict == "replace":
                    conn.execute(f'DROP TABLE "{table_name}"')
                    self.schema_manager.create_table(table_name, df, conn)
                    action = "replaced"
                elif on_conflict == "append":
                    if not self.schema_manager.schemas_match(table_name, df):
                        raise ValueError(
                            f"Schema mismatch: CSV columns don't match "
                            f"existing table '{table_name}'."
                        )
                    action = "appended"
                else:
                    raise ValueError(f"Unknown on_conflict value: {on_conflict}")
            else:
                self.schema_manager.create_table(table_name, df, conn)
                action = "created"

            rows_inserted = self._insert_rows(conn, table_name, df)

        return {"table": table_name, "rows_inserted": rows_inserted,
                "action": action}
    
    def _insert_rows(self, conn: sqlite3.Connection,
                     table: str, df: pas.DataFrame) -> int:
        """Insert DataFrame rows one by one - no df.to_sql()."""
        cols = ", ".join(f'"{c}"' for c in df.columns)
        placeholders = ", ".join("?" for _ in df.columns)
        sql = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})'

        count = 0 
        for rows in df.itertuples(index=False, name=None):
            conn.execute(sql, rows)
            count += 1

        return count