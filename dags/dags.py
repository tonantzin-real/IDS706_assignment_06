from __future__ import annotations
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil
from faker import Faker
import pandas as pd
import matplotlib.pyplot as plt


OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = "employees"

default_args = {"owner": "IDS706", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="pipeline_assignment_06",
    start_date=datetime(2025, 10, 1),
    schedule="@once",  # "00 10 * * *",
    catchup=False,
) as dag:

    @task()
    def fetch_customers(output_dir: str = OUTPUT_DIR, quantity=100) -> None:
        data = pd.read_csv("/opt/airflow/data/source_data/df_Customers.csv")

        # Transform something
        data["customer_zip_code_prefix"] = data["customer_zip_code_prefix"] * 100

        filepath = os.path.join(output_dir, "modified_data/customers.csv")

        data.to_csv(filepath, index=False)
        print(f"Data saved to {filepath}")

        return filepath

    @task()
    def fetch_orders(output_dir: str = OUTPUT_DIR, quantity=100) -> str:
        data = pd.read_csv("/opt/airflow/data/source_data/df_Orders.csv")

        # Transform something
        data["order_purchase_timestamp"] = pd.to_datetime(
            data["order_purchase_timestamp"]
        )
        data["order_purchase_timestamp"] += pd.Timedelta(days=1)

        filepath = os.path.join(output_dir, "modified_data/orders.csv")

        data.to_csv(filepath, index=False)
        print(f"Order data saved to {filepath}")
        return filepath

    @task()
    def merge_csvs(
        customer_path: str, order_path: str, output_dir: str = OUTPUT_DIR
    ) -> str:
        merged_path = os.path.join(output_dir, "modified_data/merged_data.csv")

        df_customers = pd.read_csv(customer_path)
        df_orders = pd.read_csv(order_path)

        merged_data = pd.merge(df_customers, df_orders)

        merged_data.to_csv(merged_path, index=False)
        print(f"Merged CSV saved to {merged_path}")
        return merged_path

    @task()
    def load_csv_to_pg(
        conn_id: str,
        csv_path: str,
        table: str = "customers_orders",
        append: bool = True,
    ) -> int:
        schema = "customer"

        # Reading the csv file
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = [
                tuple((r.get(col, "") or None) for col in fieldnames) for r in reader
            ]

        if not rows:
            print("No rows found in CSV; nothing to insert.")
            return 0

        # Sql query preparations
        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        create_table = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {', '.join([f'{col} TEXT' for col in fieldnames])}
            );
        """

        delete_rows = f"DELETE FROM {schema}.{table};" if not append else None

        insert_sql = f"""
            INSERT INTO {schema}.{table} ({', '.join(fieldnames)})
            VALUES ({', '.join(['%s' for _ in fieldnames])});
        """

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)

                if delete_rows:
                    cur.execute(delete_rows)
                    print(f"Cleared existing rows in {schema}.{table}")

                cur.executemany(insert_sql, rows)
                conn.commit()

            inserted = len(rows)
            print(f"Inserted {inserted} rows into {schema}.{table}")
            return inserted

        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @task()
    def clear_folder(folder_path: str = "/opt/airflow/data/modified_data") -> None:
        """
        Delete all files and subdirectories inside a folder.
        Keeps the folder itself.
        """

        if not os.path.exists(folder_path):
            print(f"Folder {folder_path} does not exist.")
            return

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"Removed directory: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

        print("Clean process completed!")

    @task
    def perform_visualization(
        conn_id: str,
        table: str = "customers_orders",
        output_dir: str = OUTPUT_DIR,
        schema="customer",
    ) -> str:

        image_path = os.path.join(output_dir, "img/orders_by_zip.png")

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()

        query = f'SELECT * FROM "{schema}"."{table}";'
        data = pd.read_sql(query, conn)
        conn.close()

        state_orders = data.groupby("customer_state").agg({"order_id": "count"})
        state_orders = state_orders.rename(columns={"order_id": "num_orders"})
        top_10_states = state_orders.sort_values(by="num_orders", ascending=False).head(
            10
        )
        plt.figure(figsize=(12, 6))
        plt.bar(top_10_states.index, top_10_states["num_orders"])
        plt.title("Top 10 States by Number of Orders")
        plt.xlabel("State")
        plt.ylabel("Number of Orders")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

        plt.savefig(image_path, bbox_inches="tight")
        plt.close()

        print(f"Visualization saved to {image_path}")

    customers_file = fetch_customers()
    orders_file = fetch_orders()
    merged_path = merge_csvs(customers_file, orders_file)

    load_to_database = load_csv_to_pg(
        conn_id="Postgres", csv_path=merged_path, table=TARGET_TABLE
    )

    create_visualization = perform_visualization(conn_id="Postgres", table=TARGET_TABLE)

    clean_folder = clear_folder()

    load_to_database >> create_visualization >> clean_folder
