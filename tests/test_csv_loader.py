import os
import pytest
import pandas as pd
from database.database import getconnection
from database.schema_manager import SchemaManager
from database.csv_loader import CSVLoader

REAL_CSV = "data/Global_Cybersecurity_Threats_2015-2024.csv"


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def loaded_db(tmp_path):
    db = str(tmp_path / "test.db")
    CSVLoader(db).load(REAL_CSV, "incidents")
    return db


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "sample.csv"
    pd.DataFrame({
        "Country":                              ["USA",          "China",      "India"],
        "Year":                                 [2020,           2021,         2022],
        "Attack Type":                          ["Phishing",     "Ransomware", "DDoS"],
        "Target Industry":                      ["Finance",      "Education",  "IT"],
        "Financial Loss (in Million $)":        [120.5,          80.3,         45.0],
        "Number of Affected Users":             [500000,         773169,       120000],
        "Attack Source":                        ["Hacker Group", "Insider",    "Unknown"],
        "Security Vulnerability Type":          ["Weak Passwords","Zero-day",  "Social Engineering"],
        "Defense Mechanism Used":               ["Firewall",     "VPN",        "Antivirus"],
        "Incident Resolution Time (in Hours)":  [24,             63,           12],
    }).to_csv(path, index=False)
    return str(path)


def test_creates_incidents_and_lookup_tables(tmp_db, sample_csv):
    CSVLoader(tmp_db).load(sample_csv, "incidents")
    tables = SchemaManager(tmp_db).get_tables()
    for t in ["incidents", "attack_types", "attack_sources",
              "vulnerability_types", "defense_mechanisms"]:
        assert t in tables


def test_correct_row_count_sample(tmp_db, sample_csv):
    result = CSVLoader(tmp_db).load(sample_csv, "incidents")
    assert result["rows_inserted"] == 3


def test_correct_row_count_full_csv(tmp_db):
    result = CSVLoader(tmp_db).load(REAL_CSV, "incidents")
    assert result["rows_inserted"] == 3000


def test_on_conflict_skip(tmp_db, sample_csv):
    loader = CSVLoader(tmp_db)
    loader.load(sample_csv, "incidents")
    result = loader.load(sample_csv, "incidents", on_conflict="skip")
    assert result["action"] == "skipped"
    assert result["rows_inserted"] == 0


def test_on_conflict_replace(tmp_db, sample_csv):
    loader = CSVLoader(tmp_db)
    loader.load(sample_csv, "incidents")
    result = loader.load(sample_csv, "incidents", on_conflict="replace")
    assert result["action"] == "replaced"
    with getconnection(tmp_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    assert count == 3


def test_on_conflict_append(tmp_db, sample_csv):
    loader = CSVLoader(tmp_db)
    loader.load(sample_csv, "incidents")
    loader.load(sample_csv, "incidents", on_conflict="append")
    with getconnection(tmp_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    assert count == 6


def test_invalid_on_conflict_raises_value_error(tmp_db, sample_csv):
    loader = CSVLoader(tmp_db)
    loader.load(sample_csv, "incidents")
    with pytest.raises(ValueError):
        loader.load(sample_csv, "incidents", on_conflict="invalid")


def test_lookup_values_deduplicated(tmp_db, sample_csv):
    loader = CSVLoader(tmp_db)
    loader.load(sample_csv, "incidents")
    loader.load(sample_csv, "incidents", on_conflict="append")
    with getconnection(tmp_db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM attack_types WHERE name='Phishing'"
        ).fetchone()[0]
    assert count == 1


def test_fk_ids_not_null_in_incidents(tmp_db, sample_csv):
    CSVLoader(tmp_db).load(sample_csv, "incidents")
    with getconnection(tmp_db) as conn:
        row = conn.execute(
            "SELECT attack_type_id, source_id, vulnerability_id, defense_id "
            "FROM incidents LIMIT 1"
        ).fetchone()
    assert all(row[k] is not None for k in
               ["attack_type_id", "source_id", "vulnerability_id", "defense_id"])


def test_full_join_across_all_fk_tables(loaded_db):
    with getconnection(loaded_db) as conn:
        rows = conn.execute("""
            SELECT at.name, src.name, vt.name, dm.name
            FROM incidents i
            JOIN attack_types        at  ON i.attack_type_id  = at.id
            JOIN attack_sources      src ON i.source_id        = src.id
            JOIN vulnerability_types vt  ON i.vulnerability_id = vt.id
            JOIN defense_mechanisms  dm  ON i.defense_id       = dm.id
            LIMIT 5
        """).fetchall()
    assert len(rows) == 5


def test_all_attack_types_in_lookup(loaded_db):
    with getconnection(loaded_db) as conn:
        names = [r["name"] for r in
                 conn.execute("SELECT name FROM attack_types").fetchall()]
    for n in ["Phishing", "Ransomware", "Man-in-the-Middle",
              "DDoS", "SQL Injection", "Malware"]:
        assert n in names


def test_all_attack_sources_in_lookup(loaded_db):
    with getconnection(loaded_db) as conn:
        names = [r["name"] for r in
                 conn.execute("SELECT name FROM attack_sources").fetchall()]
    for n in ["Hacker Group", "Nation-state", "Insider", "Unknown"]:
        assert n in names


def test_no_to_sql_used_anywhere():
    import inspect
    from database import csv_loader as mod
    assert "to_sql" not in inspect.getsource(mod)