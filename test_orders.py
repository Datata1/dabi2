import os
import psycopg2
import psycopg2.extras # Für execute_values
import random
from datetime import datetime

# Datenbankverbindungsparameter aus Umgebungsvariablen laden
DB_HOST = "localhost" # Fallback auf 'db', falls nicht gesetzt
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "oltp")
DB_USER = os.getenv("DB_USER", "datata1") # Fallback auf User aus Debezium-Konfig
DB_PASSWORD = os.getenv("DB_PASSWORD", "devpassword") # Fallback auf PW aus Debezium-Konfig

# Testdaten-Konfiguration
NUM_ORDERS_TO_CREATE = 10
PRODUCT_ID_TO_ORDER = 49302
USER_IDS_POOL = [3, 5, 6, 13, 15, 18, 19]
STARTING_ORDER_ID = 900000000 


def insert_test_data():
    conn = None
    cur = None
    inserted_order_ids = []
    order_products_data_to_insert = []
    current_manual_order_id = STARTING_ORDER_ID


    try:
        # Verbindung zur PostgreSQL-Datenbank herstellen
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        print("Erfolgreich mit der PostgreSQL-Datenbank verbunden.")

        # 1. 10 Bestellungen in die 'orders'-Tabelle einfügen
        print(f"Füge {NUM_ORDERS_TO_CREATE} Bestellungen hinzu...")
        for i in range(NUM_ORDERS_TO_CREATE):
            user_id = random.choice(USER_IDS_POOL)
            order_date = datetime.now()
            tip_given = random.choice([True, False])

            script_generated_order_id = current_manual_order_id + i


            # SQL-Statement zum Einfügen in 'orders' und Abrufen der generierten order_id
            # Annahme: order_id ist ein SERIAL oder BIGSERIAL Typ
            sql_insert_order = """
                INSERT INTO public.orders (order_id, user_id, order_date, tip_given)
                VALUES (%s, %s, %s, %s);
            """
            cur.execute(sql_insert_order, (script_generated_order_id, user_id, order_date, tip_given))
            print(f"  Bestellung {i+1}/10: order_id={script_generated_order_id}, user_id={user_id}, tip_given={tip_given}")

            # Daten für 'order_products' vorbereiten
            # Für jede neue Bestellung wird das Produkt PRODUCT_ID_TO_ORDER einmal hinzugefügt
            # add_to_cart_order wird hier als 1 angenommen, da es das erste (und einzige) Produkt in dieser Testbestellung ist
            order_products_data_to_insert.append(
                (script_generated_order_id, PRODUCT_ID_TO_ORDER, 1) # (order_id, product_id, add_to_cart_order)
            )

        # 2. Alle gesammelten 'order_products'-Einträge per Bulk Insert einfügen
        if order_products_data_to_insert:
            print(f"\nFüge {len(order_products_data_to_insert)} Einträge zu 'order_products' hinzu...")
            sql_insert_order_products = """
                INSERT INTO public.order_products (order_id, product_id, add_to_cart_order)
                VALUES %s;
            """
            # psycopg2.extras.execute_values für effizientes Einfügen mehrerer Zeilen
            psycopg2.extras.execute_values(
                cur,
                sql_insert_order_products,
                order_products_data_to_insert # Liste von Tupeln
            )
            print(f"  {cur.rowcount} Einträge erfolgreich zu 'order_products' hinzugefügt.")

        # Änderungen committen
        conn.commit()
        print("\nAlle Testdaten erfolgreich committet.")

    except (Exception, psycopg2.Error) as error:
        print(f"Fehler bei der Verbindung oder Operation mit PostgreSQL: {error}")
        if conn:
            conn.rollback() # Änderungen zurückrollen bei Fehler
            print("Transaktion wurde zurückgerollt.")
    finally:
        # Verbindung schließen
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("PostgreSQL-Verbindung geschlossen.")

if __name__ == "__main__":
    print("Starte Skript zum Einfügen von Testbestellungen...")
    insert_test_data()
    print("Skript beendet.")