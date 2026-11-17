
import requests
import json
import csv
from datetime import datetime
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def fetch_products():

    BASE_URL = "https://dummyjson.com"

    # Step 1: Fetch Products
    products_response = requests.get(
        f"{BASE_URL}/products?limit=0",
        timeout=30
    )
    products_response.raise_for_status()

    products_data = products_response.json()
    products = products_data["products"]

    for product in products:
         product["brand"] = product.get("brand")
    


    # Step 2: Fetch Customers
    customers_response = requests.get(
        f"{BASE_URL}/users?limit=0",
        timeout=30
    )
    customers_response.raise_for_status()

    customers_data = customers_response.json()
    customers = customers_data["users"]

    # Step 3: Fetch Carts
    carts_response = requests.get(
        f"{BASE_URL}/carts?limit=0",
        timeout=30
    )
    carts_response.raise_for_status()

    carts_data = carts_response.json()
    carts = carts_data["carts"]

    # Transform Carts into Orders
    orders = []

    for cart in carts:
        orders.append({
            "order_id": cart["id"],
            "customer_id": cart["userId"],
            "total_amount": cart["total"],
            "discounted_total": cart["discountedTotal"],
            "total_products": cart["totalProducts"],
            "total_quantity": cart["totalQuantity"]
        })

    # Transform Cart Products into Order Items
    order_items = []

    for cart in carts:
        for product in cart["products"]:
            order_items.append({
                "order_id": cart["id"],
                "product_id": product["id"],
                "product_title": product["title"],
                "quantity": product["quantity"],
                "unit_price": product["price"],
                "line_total": product["total"]
            })

    print(f"Total orders created: {len(orders)}")
    print(f"Total order items created: {len(order_items)}")

    # Step 4: Create Output Directory
    output_dir = Path("/opt/airflow/dags/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save Orders CSV
    with open(
        output_dir / "orders.csv",
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=orders[0].keys()
        )
        writer.writeheader()
        writer.writerows(orders)

    # Save Order Items CSV
    with open(
        output_dir / "order_items.csv",
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=order_items[0].keys()
        )
        writer.writeheader()
        writer.writerows(order_items)

    # Step 5: Save Products JSON
    with open(
        output_dir / "products.json",
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(products, file, indent=4)

    # Step 6: Save Customers JSON
    with open(
        output_dir / "customers.json",
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(customers, file, indent=4)

    # Step 7: Save Carts JSON
    with open(
        output_dir / "carts.json",
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(carts, file, indent=4)

    # Step 8: Print Record Counts
    print(f"Total products fetched: {len(products)}")
    print(f"Total customers fetched: {len(customers)}")
    print(f"Total carts fetched: {len(carts)}")

    # Step 9: PostgreSQL Connection
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_ecommerce"
    )

    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:

        # Step 10: Create Relational PostgreSQL Tables

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id BIGINT PRIMARY KEY,
                title TEXT,
                category TEXT,
                price NUMERIC(12,2),
                rating NUMERIC(5,2),
                stock INTEGER,
                brand TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id BIGINT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                phone TEXT,
                age INTEGER,
                gender TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS carts (
                cart_id BIGINT PRIMARY KEY,
                user_id BIGINT,
                total NUMERIC(12,2),
                discounted_total NUMERIC(12,2),
                total_products INTEGER,
                total_quantity INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id BIGINT PRIMARY KEY,
                customer_id BIGINT,
                total_amount NUMERIC(12,2),
                discounted_total NUMERIC(12,2),
                total_products INTEGER,
                total_quantity INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                order_id BIGINT,
                product_id BIGINT,
                product_title TEXT,
                quantity INTEGER,
                unit_price NUMERIC(12,2),
                line_total NUMERIC(12,2)
            )
        """)

        conn.commit()

        print("PostgreSQL tables checked successfully!")

        # Insert Products
        cursor.executemany("""
            INSERT INTO products (
                product_id, title, category, price,
                rating, stock, brand
            )
            VALUES (
                %(id)s, %(title)s, %(category)s, %(price)s,
                %(rating)s, %(stock)s, %(brand)s
            )
            ON CONFLICT (product_id) DO UPDATE SET
                title = EXCLUDED.title,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                rating = EXCLUDED.rating,
                stock = EXCLUDED.stock,
                brand = EXCLUDED.brand
        """, products)

        # Insert Customers
        cursor.executemany("""
            INSERT INTO customers (
                customer_id, first_name, last_name,
                email, phone, age, gender
            )
            VALUES (
                %(id)s, %(firstName)s, %(lastName)s,
                %(email)s, %(phone)s, %(age)s, %(gender)s
            )
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                age = EXCLUDED.age,
                gender = EXCLUDED.gender
        """, customers)

        # Insert Carts
        cursor.executemany("""
            INSERT INTO carts (
                cart_id, user_id, total, discounted_total,
                total_products, total_quantity
            )
            VALUES (
                %(id)s, %(userId)s, %(total)s,
                %(discountedTotal)s, %(totalProducts)s,
                %(totalQuantity)s
            )
            ON CONFLICT (cart_id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                total = EXCLUDED.total,
                discounted_total = EXCLUDED.discounted_total,
                total_products = EXCLUDED.total_products,
                total_quantity = EXCLUDED.total_quantity
        """, carts)

        conn.commit()

        print("Products inserted:", len(products))
        print("Customers inserted:", len(customers))
        print("Carts inserted:", len(carts))

        # Load Orders CSV
        with open(
            output_dir / "orders.csv",
            "r",
            encoding="utf-8"
        ) as file:

            reader = csv.DictReader(file)
            orders_rows = list(reader)

        # Load Order Items CSV
        with open(
            output_dir / "order_items.csv",
            "r",
            encoding="utf-8"
        ) as file:

            reader = csv.DictReader(file)
            order_items_rows = list(reader)

        # Insert Orders
        cursor.executemany("""
            INSERT INTO orders (
                order_id, customer_id, total_amount,
                discounted_total, total_products, total_quantity
            )
            VALUES (
                %(order_id)s, %(customer_id)s, %(total_amount)s,
                %(discounted_total)s, %(total_products)s,
                %(total_quantity)s
            )
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                total_amount = EXCLUDED.total_amount,
                discounted_total = EXCLUDED.discounted_total,
                total_products = EXCLUDED.total_products,
                total_quantity = EXCLUDED.total_quantity
        """, orders_rows)

        # Replace Order Items on Every Pipeline Run
        cursor.execute("DELETE FROM order_items")

        # Insert Order Items
        cursor.executemany("""
            INSERT INTO order_items (
                order_id, product_id, product_title,
                quantity, unit_price, line_total
            )
            VALUES (
                %(order_id)s, %(product_id)s, %(product_title)s,
                %(quantity)s, %(unit_price)s, %(line_total)s
            )
        """, order_items_rows)

        conn.commit()

        print("Orders inserted:", len(orders_rows))
        print("Order Items inserted:", len(order_items_rows))
        print("PostgreSQL data load successful!")

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()

    print("All API data saved successfully.")


# Airflow DAG

with DAG(
    dag_id="api_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["api", "data_ingestion"],
) as dag:

    fetch_products_task = PythonOperator(
        task_id="fetch_products",
        python_callable=fetch_products,
    )

