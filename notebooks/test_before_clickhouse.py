# capture_db_state.py
# Führen Sie dieses Skript VOR und NACH Ihrer Datensimulation aus, 
# um die Änderungen im Data Warehouse deutlich zu machen.

import clickhouse_connect
import pandas as pd
import os
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align

# --- 1. Konfiguration: Schema-Annahmen anpassen ---
# Diese Konfiguration sollte mit Ihrem Validierungsskript übereinstimmen.

# -- Verbindungsdetails --
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "devpassword")
CLICKHOUSE_DATABASE = "default_marts"

# -- Tabellennamen --
DIM_PRODUCTS_TABLE = "dim_products"
FACT_ORDERS_TABLE = "f_orders"
FACT_ORDER_PRODUCTS_TABLE = "f_order_lines"

# -- Spaltennamen --
PRODUCT_NK_COL = "product_id"
PRODUCT_NAME_COL = "product_name"
VALID_TO_COL = "dbt_valid_to"

# -- ID des Produkts, das wir beobachten --
PRODUCT_ID_TO_WATCH = 35708

console = Console()

def print_df_as_table(df: pd.DataFrame, title: str):
    """Druckt einen Pandas DataFrame als eine Rich-Tabelle."""
    if df.empty:
        console.print(Panel(f"[yellow]Keine Daten für '{title}' gefunden.[/yellow]", border_style="yellow"))
        return

    table = Table(title=Text(title, style="bold cyan"), show_header=True, header_style="bold magenta", border_style="green")
    
    for column in df.columns:
        table.add_column(str(column), justify="left")
        
    for _, row in df.iterrows():
        table.add_row(*[str(item) for item in row])
        
    console.print(Align.center(table))

def capture_state():
    """Erfasst und zeigt den aktuellen Zustand der relevanten DWH-Tabellen an."""
    console.print(Panel(Align.center(Text("📷 Datenbank-Zustandserfassung (Snapshot) 📷", style="bold blue on white")), border_style="blue"))

    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        console.print("\n[green]✅ Erfolgreich mit ClickHouse verbunden![/green]")

        # --- Schritt 1: Gesamtzahlen der Zeilen erfassen ---
        console.print("\n[bold]1. Aktuelle Größe der Kern-Tabellen:[/bold]")
        
        counts_query = f"""
        SELECT
            (SELECT count() FROM {DIM_PRODUCTS_TABLE}) AS total_products,
            (SELECT count() FROM {FACT_ORDERS_TABLE}) AS total_orders,
            (SELECT count() FROM {FACT_ORDER_PRODUCTS_TABLE}) AS total_order_lines
        """
        counts_df = client.query_df(counts_query)
        print_df_as_table(counts_df, "Tabellengrößen")

        # --- Schritt 2: Den Zustand eines spezifischen Datensatzes erfassen ---
        console.print(f"\n[bold]2. Aktueller Zustand des Produkts mit der ID [yellow]{PRODUCT_ID_TO_WATCH}[/yellow]:[/bold]")

        # Wir fragen nur den aktuell gültigen Eintrag ab (valid_to IS NULL)
        product_state_query = f"""
        SELECT {PRODUCT_NK_COL}, {PRODUCT_NAME_COL}, {VALID_TO_COL}
        FROM {DIM_PRODUCTS_TABLE}
        WHERE {PRODUCT_NK_COL} = %(product_id)s AND {VALID_TO_COL} IS NULL
        """
        params = {'product_id': PRODUCT_ID_TO_WATCH}
        product_df = client.query_df(product_state_query, parameters=params)

        print_df_as_table(product_df, f"Aktuell gültiger Eintrag für Produkt {PRODUCT_ID_TO_WATCH}")
        
        console.print(Panel("[bold green]Snapshot abgeschlossen. Führen Sie nun Ihre Simulation aus und starten Sie dieses Skript erneut.[/bold green]"))

    except Exception:
        console.print("\n[bold red]❌ EIN FEHLER IST AUFGETRETEN[/bold red]")
        console.print_exception(show_locals=False)

if __name__ == "__main__":
    # Bevor Sie das Skript ausführen, stellen Sie sicher, dass 'rich' installiert ist:
    # pip install rich
    capture_state()
