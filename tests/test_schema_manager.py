import pytest
import pandas as pas
from database.database import getconnection
from database.schema_manager import SchemaManager
from database.csv_loader import CSVLoader

REAL_CSV = "data/Global_Cybersecurity_Threats_2015-2024.csv"

@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")

@pytest.fixture
def loaded_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    CSVLoader(db_path).load(REAL_CSV, "incidents")
    return db_path

def test_new_db_has_no_table(tmp_db):
    sm = SchemaManager(tmp_db)
    assert sm.get_tables() == []

def test_tables_exist_after_loading_data(loaded_db):
    sm = SchemaManager(loaded_db)
    tables = sm.get_tables()

    expected_tables = ["incidents", "attack_types", "attack_sources",
              "vulnerability_types", "defense_mechanisms"]
    
    for table in expected_tables:
        assert table in tables 

def test_table_exists_true_case(loaded_db):
    sm = SchemaManager(loaded_db)
    assert sm.table_exists("incidents") is True

def test_table_exists_returns_false_for_unkown(loaded_db):
    sm = SchemaManager(loaded_db)
    assert sm.table_exists("nonexistent") is False


def test_incidents_table_has_expected_columns(loaded_db):
    sm = SchemaManager(loaded_db)

    columns = [col["name"] for col in sm.get_table_schema("incidents")]
    
    expected_columns= ["id", "year", "country", "target_industry",
                     "financial_loss_usd_m", "num_affected_users",
                     "resolution_hours", "attack_type_id",
                     "source_id", "vulnerability_id", "defense_id"]
    
    for col in expected_columns:
        assert col in columns


def test_column_tyoe_are_correct(loaded_db):
    sm = SchemaManager(loaded_db)

    column_types = {
        col["name"]: col["type"] 
        for col in sm.get_table_schema("incidents")
    }

    assert column_types["year"] == "INTEGER"
    assert column_types["financial_loss_usd_m"] == "REAL"
    assert column_types["country"] == "TEXT"


def test_get_all_schemas_returns_all_tables(loaded_db):
    sm = SchemaManager(loaded_db)
    schemas = sm.get_all_schemas()
    
    for table in [
        "incidents", 
        "attack_types", 
        "attack_sources",
        "vulnerability_types", 
        "defense_mechanisms"]:
     assert table in schemas

     assert len(schemas["incidents"]) > 0


def test_infer_sql_type_mapping(tmp_db):
    sm = SchemaManager(tmp_db)
    assert sm.infere_sql_type("int64")   == "INTEGER"
    assert sm.infere_sql_type("float64") == "REAL"
    assert sm.infere_sql_type("object")  == "TEXT"
    assert sm.infere_sql_type("bool")    == "INTEGER"
    assert sm.infere_sql_type("unknown") == "TEXT"


def test_create_table_adds_id_primary_key(tmp_db):
    sm = SchemaManager(tmp_db)
    df = pas.DataFrame({"name": ["a"], "value": [1]})
    with getconnection(tmp_db) as conn:
        sm.create_table("tbl", df, conn)
    assert any(c["name"] == "id" for c in sm.get_table_schema("tbl"))

def test_schemas_match_returns_true(tmp_db):
    sm = SchemaManager(tmp_db)
    df = pas.DataFrame({"country": ["USA"], "year": [2020]})
    with getconnection(tmp_db) as conn:
        sm.create_table("tbl", df, conn)
    assert sm.schemas_match("tbl", df) is True


def test_schemas_match_returns_false(tmp_db):
    sm = SchemaManager(tmp_db)
    df1 = pas.DataFrame({"country": ["USA"], "year": [2020]})
    df2 = pas.DataFrame({"different_col": ["x"]})
    with getconnection(tmp_db) as conn:
        sm.create_table("tbl", df1, conn)
    assert sm.schemas_match("tbl", df2) is False