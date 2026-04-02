import sys
import os

# lets imports work even if the file is run from another folder
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.query_service import QueryService
from app.llm_adapter import LLMAdapter, LLMError
from app.sql_validator import ValidationError


def print_menu():
    """Print the main menu options."""
    print()
    print("=" * 40)
    print("       DataQuery-engine — Main Menu")
    print("=" * 40)
    print("  1. Load a CSV file")
    print("  2. Ask a question (natural language)")
    print("  3. List tables and columns")
    print("  4. Exit")
    print("=" * 40)


def handle_load_csv(qs):
    print()
    csv_path = input("Enter path to CSV file: ").strip()

    if not os.path.exists(csv_path):
        print(f"  Error: File not found — '{csv_path}'")
        return

    table_name = input("Enter table name to load into: ").strip()
    if not table_name:
        print("  Error: Table name cannot be empty.")
        return

    print("  If table already exists:")
    print("    a. append   — add rows if schema matches")
    print("    b. replace  — clear and reload")
    print("    c. skip     — do nothing")
    choice = input("  Choose (a/b/c) [default: a]: ").strip().lower()

    conflict_map = {
        "a": "append", 
        "b": "replace", 
        "c": "skip", 
        "": "append"
        }
    on_conflict = conflict_map.get(choice, "append")

    try:
        result = qs.load_csv(csv_path, table_name, on_conflict)
        print()
        print(f"  Done! {result['rows_inserted']} rows {result['action']}"
              f" into '{result['table']}'.")
    except ValueError as e:
        print(f"  Error: {e}")
    except Exception as e:
        print(f"  Unexpected error: {e}")


def handle_ask_question(qs, adapter):
    print()

    # Make sure there are tables to query before asking
    tables = qs.get_tables()
    if not tables:
        print("  No tables loaded yet. Please load a CSV first (option 1).")
        return

    question = input("Ask your question: ").strip()
    if not question:
        print("  Error: Question cannot be empty.")
        return

    print("  Generating SQL...")

    try:
        schema = qs.get_schema()
        sql = adapter.generate_sql(question, schema)

        print(f"  Generated SQL: {sql}")
        print()

        rows = qs.execute_query(sql)

        if not rows:
            print("  No results found.")
            return

        print(f"  {len(rows)} row(s) returned:")
        print()


        # only show first 20 rows so output does not get messy
        for row in rows[:20]:
            print("  " + " | ".join(f"{str(v):<20}" for v in row))

        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more rows.")

    except LLMError as e:
        print(f"  LLM error: {e}")
    except ValidationError as e:
        print(f"  Validation error — unsafe query blocked: {e}")
    except Exception as e:
        print(f"  Error: {e}")


def handle_list_tables(qs):
    print()
    schema = qs.get_schema()

    user_tables = {
        table_name: columns
        for table_name, columns in schema.items()
        if table_name != "sqlite_sequence"
    }

    if not user_tables:
        print("  No tables loaded yet. Please load a CSV first (option 1).")
        return

    for table_name, columns in user_tables.items():
        print(f"  Table: {table_name}")
        for col in columns:
            print(f"    - {col['name']} ({col['type']})")
        print()



def main():
    print()
    print("  Welcome to DataQuery-Engine")
    print("  Natural language interface for data")

    # Initialize the core services
    qs = QueryService()
    adapter = LLMAdapter()

    while True:
        print_menu()
        choice = input("Enter choice (1-4): ").strip()

        if choice == "1":
            handle_load_csv(qs)

        elif choice == "2":
            handle_ask_question(qs, adapter)

        elif choice == "3":
            handle_list_tables(qs)

        elif choice == "4":
            print()
            print("  Goodbye!")
            print()
            break

        else:
            print()
            print("  Invalid choice — please enter 1, 2, 3, or 4.")


if __name__ == "__main__":
    main()