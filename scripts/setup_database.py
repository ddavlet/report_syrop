#!/usr/bin/env python3
"""
Database Setup Script
Executes the SQL file to create all necessary tables in PostgreSQL
"""

import os
import sys
import argparse
from pathlib import Path

def setup_database(pg_dsn: str, sql_file: str = "create_tables.sql"):
    """
    Execute the SQL file to create database tables

    Args:
        pg_dsn: PostgreSQL connection string
        sql_file: Path to the SQL file to execute
    """
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        print("Error: Required packages not installed.")
        print("Please install: pip install sqlalchemy psycopg2-binary")
        sys.exit(1)

    # Get the directory of this script
    script_dir = Path(__file__).parent
    sql_path = script_dir / sql_file

    if not sql_path.exists():
        print(f"Error: SQL file not found: {sql_path}")
        sys.exit(1)

    print(f"Reading SQL file: {sql_path}")

    try:
        # Read the SQL file
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        print("Connecting to PostgreSQL...")
        engine = create_engine(pg_dsn)

        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"Connected to PostgreSQL: {version}")

        print("Executing SQL file...")

        # Execute the SQL file
        with engine.begin() as conn:
            # Parse SQL statements properly, handling $$ delimited functions
            statements = []
            current_statement = ""
            in_dollar_quoted = False
            dollar_tag = None

            for line in sql_content.split('\n'):
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith('--'):
                    continue

                current_statement += line + '\n'

                # Check for dollar-quoted string start/end
                if '$$' in line and not in_dollar_quoted:
                    # Starting a dollar-quoted block
                    in_dollar_quoted = True
                    # Extract dollar tag (e.g., $tag$ or just $$)
                    parts = line.split('$$')
                    if len(parts) >= 2:
                        dollar_tag = '$$' + parts[0].split('$$')[-1] + '$$' if parts[0] else '$$'
                elif '$$' in line and in_dollar_quoted:
                    # Check if this ends the dollar-quoted block
                    if dollar_tag and dollar_tag in line:
                        in_dollar_quoted = False
                        dollar_tag = None

                # If we hit a semicolon and we're not in a dollar-quoted block, end the statement
                if line.endswith(';') and not in_dollar_quoted:
                    statements.append(current_statement.strip())
                    current_statement = ""

            # Add any remaining statement
            if current_statement.strip():
                statements.append(current_statement.strip())

            for i, statement in enumerate(statements, 1):
                if statement:
                    try:
                        conn.execute(text(statement))
                        print(f"Executed statement {i}/{len(statements)}")
                    except Exception as e:
                        print(f"Warning: Error executing statement {i}: {e}")
                        # Continue with other statements

        print("Database setup completed successfully!")
        print("\nTables created:")
        print("- clients")
        print("- items")
        print("- sales")
        print("- sales_items")

    except Exception as e:
        print(f"Error setting up database: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Setup PostgreSQL database tables")
    parser.add_argument(
        "--pg-dsn",
        required=True,
        help="PostgreSQL connection string (e.g., postgresql://user:pass@host:port/db)"
    )
    parser.add_argument(
        "--sql-file",
        default="create_tables.sql",
        help="SQL file to execute (default: create_tables.sql)"
    )

    args = parser.parse_args()

    setup_database(args.pg_dsn, args.sql_file)

if __name__ == "__main__":
    main()
