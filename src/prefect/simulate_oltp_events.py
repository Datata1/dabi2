# simulate_demo_data_sequential.py

import psycopg2
from faker import Faker
import random
import time
import os
from datetime import datetime, timedelta
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.text import Text

# --- 1. Konfiguration ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "oltp")
DB_USER = os.getenv("DB_USER", "datata1")
DB_PASS = os.getenv("DB_PASS", "devpassword")

# --- Hardcodierte IDs für die Demo-ÄNDERUNGEN ---
PRODUCTS_TO_RENAME = {35708: "Chocolate Coconut Chew Flavored Bar dabi rockt", 49302: "Bulgarian Yogurt asd"}
DEPARTMENTS_TO_RENAME = {1: "frozen sa", 16: "dairy eggs asdad", 19: "snack asd"}
AISLES_TO_RENAME = {3: "energy granola bars sind legga", 4: "instant food asd"}
USER_IDS_FOR_DEMO = [7309, 1167, 12544, 23955, 42392, 55114, 67431, 888, 9997, 103]
ORDER_COUNT = 100

# --- Hardcodierte ID-Pools für die BESTELLUNGEN ---
PRODUCT_IDS_FOR_ORDERS = [6615, 196, 35108, 4086, 20574]
AISLE_IDS_FOR_ORDERS = [1, 2, 5]
DEPARTMENT_IDS_FOR_ORDERS = [4, 7, 13]

fake = Faker('de-DE')
console = Console()

def get_next_id(cursor, table, column):
    cursor.execute(f"SELECT MAX({column}) FROM {table}")
    max_id = cursor.fetchone()[0]
    return (max_id or 0) + 1

def run_demo_scenario():
    """Führt das Demo-Szenario mit sequentiellen Commits aus."""
    connection = None
    try:
        connection = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        connection.autocommit = False
        cursor = connection.cursor()
        console.print(Panel("[bold green]✅ Erfolgreich mit PostgreSQL-DB verbunden![/bold green]"))

        # --- PHASE 1: SEQUENTIELLE DIMENSIONS-ÄNDERUNGEN ---
        console.print(Panel("Phase 1: Sequentielle Dimensions-Änderungen", style="bold blue", expand=False))

        # Schritt 1.1: Gänge (Aisles) ändern und committen
        console.print("\n[cyan]Schritt 1.1: Ändere Aisle-Namen...[/cyan]")
        for aid, new_name in AISLES_TO_RENAME.items():
            cursor.execute("UPDATE aisles SET aisle = %s WHERE aisle_id = %s", (new_name, aid))
        connection.commit()
        console.print("[green]✅ Aisle-Änderungen committet.[/green]")
        time.sleep(1)

        # Schritt 1.2: Abteilungen (Departments) ändern und committen
        console.print("\n[cyan]Schritt 1.2: Ändere Department-Namen...[/cyan]")
        for did, new_name in DEPARTMENTS_TO_RENAME.items():
            cursor.execute("UPDATE departments SET department = %s WHERE department_id = %s", (new_name, did))
        connection.commit()
        console.print("[green]✅ Department-Änderungen committet.[/green]")
        time.sleep(1)

        # Schritt 1.3: Produkt-Änderungen vorbereiten (umbenennen & neu erstellen)
        console.print("\n[cyan]Schritt 1.3: Ändere Produkt-Namen und erstelle neue Produkte...[/cyan]")
        for pid, new_name in PRODUCTS_TO_RENAME.items():
            cursor.execute("UPDATE products SET product_name = %s WHERE product_id = %s", (new_name, pid))
        
        newly_created_product_ids = []
        next_product_id = get_next_id(cursor, "products", "product_id")
        target_aisle_ids = list(AISLES_TO_RENAME.keys())
        target_dept_ids = list(DEPARTMENTS_TO_RENAME.keys())
        for i in range(2):
            new_product_name = f"Demo Produkt - {fake.word().capitalize()} {fake.word().capitalize()}"
            aisle_id = target_aisle_ids[i % len(target_aisle_ids)]
            department_id = target_dept_ids[i % len(target_dept_ids)]
            cursor.execute("INSERT INTO products (product_id, product_name, aisle_id, department_id) VALUES (%s, %s, %s, %s)", (next_product_id, new_product_name, aisle_id, department_id))
            newly_created_product_ids.append(next_product_id)
            next_product_id += 1
        
        connection.commit()
        console.print("[green]✅ Produkt-Änderungen (Rename & New) committet.[/green]")
        time.sleep(1)

        # --- PHASE 2: BESTELL-WELLE ---
        console.print(Panel("Phase 2: Bestell-Welle simulieren", style="bold blue", expand=False))
        
        special_products_pool = list(PRODUCTS_TO_RENAME.keys()) + newly_created_product_ids
        other_products_set = set(PRODUCT_IDS_FOR_ORDERS)
        if AISLE_IDS_FOR_ORDERS or DEPARTMENT_IDS_FOR_ORDERS:
            query = "SELECT product_id FROM products WHERE aisle_id = ANY(%s) OR department_id = ANY(%s);"
            cursor.execute(query, (AISLE_IDS_FOR_ORDERS, DEPARTMENT_IDS_FOR_ORDERS))
            products_from_cats = [row[0] for row in cursor.fetchall()]
            other_products_set.update(products_from_cats)
        
        other_products_set.difference_update(special_products_pool)
        other_products_pool = list(other_products_set)
        
        if not special_products_pool:
            raise Exception("Der Pool der Spezial-Produkte ist leer.")

        next_order_id = get_next_id(cursor, "orders", "order_id")
        
        with Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TextColumn("({task.completed} von {task.total})")
        ) as progress:
            task = progress.add_task("[green]Erstelle Bestellungen...", total=ORDER_COUNT)
            for i in range(ORDER_COUNT):
                num_total_products = random.randint(1, 10)
                products_for_this_order = [random.choice(special_products_pool)]
                
                num_remaining = num_total_products - 1
                if num_remaining > 0 and other_products_pool:
                    products_for_this_order.extend(random.sample(other_products_pool, min(num_remaining, len(other_products_pool))))
                
                random_user_id = random.choice(USER_IDS_FOR_DEMO)
                order_timestamp = datetime.now() - timedelta(seconds=random.randint(0, 3600))
                tip_was_given = random.random() < 0.6
                
                cursor.execute("INSERT INTO orders (order_id, user_id, order_date, tip_given) VALUES (%s, %s, %s, %s)", (next_order_id, random_user_id, order_timestamp, tip_was_given))
                for j, product_id in enumerate(products_for_this_order):
                    cursor.execute("INSERT INTO order_products (order_id, product_id, add_to_cart_order) VALUES (%s, %s, %s)", (next_order_id, product_id, j + 1))
                
                next_order_id += 1
                progress.update(task, advance=1)
                time.sleep(0.02) # Kurze Pause für den visuellen Effekt

        connection.commit()
        console.print("[green]✅ Bestellungen committet.[/green]\n")
        console.print(Panel("[bold green]🚀 GESAMTE SIMULATION ERFOLGREICH ABGESCHLOSSEN 🚀[/bold green]"))

    except psycopg2.Error as e:
        console.print("\n[bold red]❌ EIN DATENBANK-FEHLER IST AUFGETRETEN[/bold red]")
        console.print(f"[red]{e}[/red]")
        if connection: connection.rollback()
    except Exception:
        console.print("\n[bold red]❌ EIN ALLGEMEINER FEHLER IST AUFGETRETEN[/bold red]")
        console.print_exception(show_locals=False)
        if connection: connection.rollback()
    finally:
        if connection:
            connection.close()
if __name__ == "__main__":
    # Bevor Sie das Skript ausführen, stellen Sie sicher, dass 'rich' installiert ist:
    # pip install rich
    run_demo_scenario()
