import pymysql
import psycopg2
import re

# ==== CONFIGURATION ====

# OceanBase connection info (MySQL compatible) - considered New DB
NEW_DB_HOST = "t6ysbapr4yf1c.aws-ap-northeast-1-internet.oceanbase.cloud"
DB_PORT = 3306
DB_USER = "lanazhou"
DB_PASSWORD = "dCniCg6hwRDA"

# PostgreSQL connection info - used only for PostgreSQL vs New DB comparison
POSTGRES_HOST = "3.114.119.91"
POSTGRES_PORT = 5432
POSTGRES_DB = "qa_site"
POSTGRES_USER = "qa_user"
POSTGRES_PASSWORD = "M5lRLZ8SHja0LD"

# Old MySQL DB connection info - considered Old DB
OLD_DB_HOST = "fortress-devs.camuzoymf9jw.ap-northeast-1.rds.amazonaws.com"

# Tables to compare PostgreSQL vs New DB (OceanBase) (subset)
POSTGRES_COMPARE_TABLES = ["loyalty_payouts", "loyalty_players"]

# Ignore lists
IGNORE_TABLES_POSTGRES = [
    "databasechangelog",
    "databasechangeloglock",
]

IGNORE_TABLES_OLD_DB = [
    "databasechangelog",
    "databasechangeloglock",
    "access_keys",
    "bet_history_msg_error_log",
    "bet_history_msg_log",
    "bet_transfer_msg_error_log",
    "bet_transfer_msg_log",
    "credit_flow_old",
    "player_wallet_balances",
    "sport_unsettled_bet_msg_error_log",
    "sport_unsettled_bet_msg_log",
    "transfer_history",
    "transfer_history_old",
    "turnover_history",
    "unsettled_bet_msg_error_log",
    "unsettled_bet_msg_log",
    "wallet_balance_stats",
    "wallet_balance",
    "player_inbox"
]

# ==== UTILITY FUNCTIONS ====

def normalize_type(col_type):
    col_type = col_type.lower().strip()
    
    col_type = col_type.replace("unsigned", "").strip()
    col_type = re.sub(r"\(.*\)", "", col_type)

    if col_type.startswith("varchar") or col_type == "text" or col_type.startswith("char") or col_type == "character varying":
        col_type = "string"

    if col_type in ("integer", "int", "int4"):
        col_type = "int"
    if col_type in ("bigint", "int8"):
        col_type = "bigint"
    if col_type in ("smallint", "int2"):
        col_type = "smallint"

    if col_type in ("boolean", "bool"):
        col_type = "tinyint"

    if col_type in ("numeric", "decimal"):
        col_type = "decimal"
    return col_type.strip()

def get_actual_schema_mysql(connection, database):
    cursor = connection.cursor()
    query = """
        SELECT table_name, column_name, column_type
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
    """
    cursor.execute(query, (database,))
    rows = cursor.fetchall()

    schema = {}
    for table, column, col_type in rows:
        table_lower = table.lower()
        column_lower = column.lower()
        schema.setdefault(table_lower, {})[column_lower] = col_type.lower()
    return schema

def get_pg_table_schema(connection, table_name, schema_name='public'):
    cursor = connection.cursor()
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    cursor.execute(query, (schema_name, table_name))
    rows = cursor.fetchall()
    schema = {col.lower(): dtype.lower() for col, dtype in rows}
    return schema

def compare_schemas(schema1, schema2, ignore_tables=None, source_name="New DB", target_name="Old DB"):
    if ignore_tables is None:
        ignore_tables = []
    ignore_tables = [t.lower() for t in ignore_tables]

    errors = []

    for table, columns in schema1.items():
        if table not in schema2:
            if table.lower() not in ignore_tables:
                errors.append(f"Missing table in {target_name}: {table}")
            continue
        for col, col_type in columns.items():
            if col not in schema2[table]:
                if table.lower() not in ignore_tables:
                    errors.append(f"Missing column '{col}' in table '{table}' in {target_name}")
            else:
                norm_col_type_1 = normalize_type(col_type)
                norm_col_type_2 = normalize_type(schema2[table][col])
                if norm_col_type_2 != norm_col_type_1:
                    if table.lower() not in ignore_tables:
                        errors.append(
                            f"Column type mismatch in '{table}.{col}': {source_name} has {col_type}, {target_name} has {schema2[table][col]}"
                        )

    for table in schema2:
        if table.lower() in ignore_tables:
            continue
        if table not in schema1:
            errors.append(f"Unexpected table in {target_name}: {table}")
            continue
        for col in schema2[table]:
            if col not in schema1[table]:
                errors.append(f"Unexpected column '{col}' in table '{table}' in {target_name}")

    return errors

# ==== DATA VALIDATION FUNCTIONS (MySQL & PostgreSQL) ====

def get_row_count_mysql(connection, table_name):
    """Fetches the total number of rows for a given table from a MySQL connection."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except pymysql.err.ProgrammingError as e:
        print(f"  - WARNING: Table '{table_name}' not found in the database. Skipping row count validation. Error: {e}")
        return -1

def get_row_count_pg(connection, table_name):
    """Fetches the total number of rows for a given table from a PostgreSQL connection."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except psycopg2.errors.UndefinedTable as e:
        print(f"  - WARNING: Table '{table_name}' not found in the database. Skipping row count validation. Error: {e}")
        return -1

def validate_mysql_row_counts(old_db_connection, new_db_connection, db_name, ignore_tables):
    """
    Compares the row counts of all tables between old MySQL and new databases.
    The new database should not have fewer rows than the old database.
    """
    print(f"\n--- Starting MySQL data validation for '{db_name}' ---")
    errors = []
    
    cursor_old = old_db_connection.cursor()
    cursor_old.execute("SHOW TABLES")
    old_db_tables = [table[0] for table in cursor_old.fetchall()]
    cursor_old.close()

    for table in old_db_tables:
        if table.lower() in [t.lower() for t in ignore_tables]:
            print(f"  - Skipping table '{table}' as it's in the ignore list.")
            continue
        
        old_count = get_row_count_mysql(old_db_connection, table)
        new_count = get_row_count_mysql(new_db_connection, table)

        if old_count == -1 or new_count == -1:
            continue

        print(f"  - Table '{table}': Old DB has {old_count} rows, New DB has {new_count} rows.")
        
        if new_count < old_count:
            errors.append(f"Row count mismatch in '{table}': New DB has {new_count} rows, which is LESS than Old DB's {old_count} rows. ❌")
        
    if errors:
        print(f"\nMySQL row count validation for '{db_name}' FAILED ❌")
        for error in errors:
            print(error)
    else:
        print(f"\nMySQL row count validation for '{db_name}' passed ✅")
    
    return errors

def validate_pg_row_counts(pg_connection, new_db_connection, db_name, tables_to_compare):
    """
    Compares the row counts of specific tables between PostgreSQL and the new database.
    The new database should not have fewer rows than the PostgreSQL database.
    """
    print(f"\n--- Starting PostgreSQL data validation for '{db_name}' ---")
    errors = []

    for table in tables_to_compare:
        pg_count = get_row_count_pg(pg_connection, table)
        new_db_count = get_row_count_mysql(new_db_connection, table)
        
        if pg_count == -1 or new_db_count == -1:
            continue

        print(f"  - Table '{table}': PostgreSQL DB has {pg_count} rows, New DB has {new_db_count} rows.")

        if new_db_count < pg_count:
            errors.append(f"Row count mismatch in '{table}': New DB has {new_db_count} rows, which is LESS than PostgreSQL's {pg_count} rows. ❌")
    
    if errors:
        print(f"\nPostgreSQL row count validation for '{db_name}' FAILED ❌")
        for error in errors:
            print(error)
    else:
        print(f"\nPostgreSQL row count validation for '{db_name}' passed ✅")
    
    return errors

# ==== MAIN EXECUTION SCRIPT ====

if __name__ == "__main__":
    try:
        # Schema and Data Validation for PostgreSQL vs New DB
        # ----------------------------------------------------
        
        print("Comparing PostgreSQL vs New DB for selected tables...")

        conn_new_db = pymysql.connect(
            host=NEW_DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database="qa_loyalty",
            ssl={"ssl": False}
        )
        schema_new_db = get_actual_schema_mysql(conn_new_db, "qa_loyalty")

        conn_pg = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

        schema_pg_subset = {}
        for tbl in POSTGRES_COMPARE_TABLES:
            schema_pg_subset[tbl.lower()] = get_pg_table_schema(conn_pg, tbl)

        schema_new_db_subset = {tbl.lower(): schema_new_db.get(tbl.lower(), {}) for tbl in POSTGRES_COMPARE_TABLES}

        differences_pg_vs_new = compare_schemas(
            schema_pg_subset,
            schema_new_db_subset,
            ignore_tables=IGNORE_TABLES_POSTGRES,
            source_name="PostgreSQL",
            target_name="New DB"
        )

        if differences_pg_vs_new:
            print("Schema comparison between PostgreSQL and New DB for selected tables FAILED ❌")
            for issue in differences_pg_vs_new:
                print(" -", issue)
        else:
            print("Schemas in PostgreSQL and New DB for selected tables are identical ✅")

        # Call the new data validation function for PostgreSQL
        validate_pg_row_counts(conn_pg, conn_new_db, "qa_loyalty", POSTGRES_COMPARE_TABLES)

        conn_new_db.close()
        conn_pg.close()

        # OceanBase (New DB) vs Old MySQL DB full comparisons
        def compare_mysql_databases(db_name):
            print(f"\nComparing New DB and Old DB for database '{db_name}'...")

            conn_new = pymysql.connect(
                host=NEW_DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=db_name,
                ssl={"ssl": False}
            )
            conn_old = pymysql.connect(
                host=OLD_DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=db_name,
                ssl={"ssl": False}
            )

            schema_new = get_actual_schema_mysql(conn_new, db_name)
            schema_old = get_actual_schema_mysql(conn_old, db_name)

            differences = compare_schemas(
                schema_new,
                schema_old,
                ignore_tables=IGNORE_TABLES_OLD_DB,
                source_name="New DB",
                target_name="Old DB"
            )

            if differences:
                print(f"Schema comparison between New DB and Old DB for '{db_name}' FAILED ❌")
                for issue in differences:
                    print(" -", issue)
            else:
                print(f"Schemas in New DB and Old DB for '{db_name}' are identical ✅")

            # Call the MySQL data validation function
            validate_mysql_row_counts(conn_old, conn_new, db_name, IGNORE_TABLES_OLD_DB)

            conn_new.close()
            conn_old.close()

        compare_mysql_databases("qa_site")
        compare_mysql_databases("qa_wallet")

    except Exception as e:
        print("Error:", e)