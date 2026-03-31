import pandas as pas
import sqlite3
from database.database import getconnection
from database.schema_manager import SchemaManager



LOOKUP_Tables = {
    "attack_type":"attack_types",
    "attack_source":"attack_sources",
    "security_vulnerability_type":"vulnerability_types",
    "defense_mechanism_used":"defense_mechanisms",
}

LOOKUP_DDL = """
CREATE TABLE IF NOT EXISTS attack_types (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS attack_sources (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS vulnerability_types (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS defense_mechanisms (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
"""
 
# Schema for the main incidents table
INCIDENTS_DDL = """
CREATE TABLE IF NOT EXISTS incidents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    country TEXT,
    target_industry TEXT,
    financial_loss_usd_m REAL,
    num_affected_users INTEGER,
    resolution_hours INTEGER,
    attack_type_id INTEGER REFERENCES attack_types(id),
    source_id INTEGER REFERENCES attack_sources(id),
    vulnerability_id INTEGER REFERENCES vulnerability_types(id),
    defense_id INTEGER REFERENCES defense_mechanisms(id)
);
"""
 

class CSVLoader:
    def __init__(self, db_path=None):
        self.db_path = db_path
        self.schema_manager = SchemaManager(db_path)

    def load(self, csv_path, table_name, on_conflict="append") -> dict:
        df = pas.read_csv(csv_path)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        df = df.rename(columns={
            "financial_loss_(in_million_$)": "financial_loss_usd_m",
            "number_of_affected_users": "num_affected_users",
            "incident_resolution_time_(in_hours)": "resolution_hours"
        })

        with getconnection(self.db_path) as conn:
            self._create_tables(conn)

            if self.schema_manager.table_exists(table_name):
                if on_conflict == "skip":
                    return {"table": table_name, "rows_inserted": 0, "action": "skipped"}
                elif on_conflict == "replace":
                    self._clear_tables(conn)
                    action = "replaced"
                elif on_conflict == "append":
                    action = "appended"
                else:
                    raise ValueError(f"Invalid on_conflict value: {on_conflict}")
            else:
                action = "created"

            rows_inserted = self._insert_rows(conn, df)

        return {"table": table_name, "rows_inserted": rows_inserted,
                "action": action}
    
    def _create_tables(self, conn):
        conn.executescript(LOOKUP_DDL)
        conn.execute(INCIDENTS_DDL)
    
    def _clear_tables(self, conn):
        conn.execute("DELETE FROM incidents")
        conn.execute("DELETE FROM attack_types")
        conn.execute("DELETE FROM attack_sources")
        conn.execute("DELETE FROM vulnerability_types")
        conn.execute("DELETE FROM defense_mechanisms")

    def _get_or_create_id(self, conn, table_name, value):
        row = conn.execute(
            f'SELECT id FROM "{table_name}" WHERE name = ?', (value,)
        ).fetchone()
        if row:
            return row["id"]
        cursor = conn.execute(
            f'INSERT INTO "{table_name}" (name) VALUES (?)', (value,)
        )
        return cursor.lastrowid
    
    def _insert_rows(self, conn, df: pas.DataFrame):
        insert_sql = """
            INSERT INTO incidents (
                year, 
                country, 
                target_industry,
                financial_loss_usd_m, 
                num_affected_users, 
                resolution_hours,
                attack_type_id, 
                source_id, 
                vulnerability_id, 
                defense_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
 
        count = 0
        for row in df.itertuples(index=False):
            attack_type_id = self._get_or_create_id(conn, "attack_types", row.attack_type)
            source_id = self._get_or_create_id(conn, "attack_sources", row.attack_source)
            vulnerability_id = self._get_or_create_id(conn, "vulnerability_types", row.security_vulnerability_type)
            defense_id = self._get_or_create_id(conn, "defense_mechanisms", row.defense_mechanism_used)

            conn.execute(insert_sql, (
                row.year,
                row.country,
                row.target_industry,
                row.financial_loss_usd_m,
                row.num_affected_users,
                row.resolution_hours,
                attack_type_id,
                source_id,
                vulnerability_id,
                defense_id,
            ))
            count += 1
 
        return count