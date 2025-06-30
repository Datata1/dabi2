# test_clickhouse_validation.py

import clickhouse_connect
import pandas as pd
import os
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.padding import Padding
from rich.align import Align
from rich.columns import Columns

# --- 1. Konfiguration: Schema-Annahmen anpassen ---
# Diese Konfiguration sollte mit Ihrem Simulationsskript übereinstimmen.

# -- Verbindungsdetails --
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "devpassword")
CLICKHOUSE_DATABASE = "default_marts"

# -- Tabellennamen --
DIM_PRODUCTS_TABLE = "dim_products"
FACT_ORDER_PRODUCTS_TABLE = "f_order_lines"

# -- Spaltennamen in der Produkt-Dimension --
PRODUCT_SK_COL = "dbt_scd_id" 
PRODUCT_NK_COL = "product_id"
PRODUCT_NAME_COL = "product_name"
AISLE_NAME_COL = "aisle_name"
DEPARTMENT_NAME_COL = "department_name"
VALID_FROM_COL = "dbt_valid_from"
VALID_TO_COL = "dbt_valid_to"

# -- Spaltennamen in der Fakten-Tabelle --
ORDER_ID_COL = "order_id"
ORDER_PRODUCT_SK_COL = "product_sk" # Fremdschlüssel zu dim_products

# -- ID des Produkts, das wir für die Demo beobachten --
PRODUCT_ID_TO_VALIDATE = 35708

console = Console()

def create_rich_table(df: pd.DataFrame, title: str, border_style="green"):
    """Erstellt ein Rich-Tabellenobjekt aus einem Pandas DataFrame."""
    if df.empty:
        return Panel(f"[yellow]Keine Ergebnisse für '{title}' gefunden.[/yellow]", border_style="yellow", expand=False)

    table = Table(title=Text(title, style="bold cyan"), show_header=True, header_style="bold magenta", border_style=border_style, expand=True)
    
    for column in df.columns:
        style = "bold yellow" if "_sk" in column or "_id" in column else "none"
        table.add_column(str(column), justify="left", style=style)
        
    for _, row in df.iterrows():
        row_style = ""
        if PRODUCT_NAME_COL in df.columns and "TEST" in str(row[PRODUCT_NAME_COL]):
            row_style = "bold green"
        table.add_row(*[str(item) for item in row], style=row_style)
        
    return table

def print_finding(text: str, emoji="✅"):
    """Druckt eine formatierte Erkenntnis für die Demo."""
    console.print(Padding(f"[bold green]{emoji} ERKENNTNIS: [/bold green]{text}", (1, 0, 0, 4)))

def print_step_header(title: str):
    """Druckt eine große, zentrierte Überschrift für einen Demo-Schritt."""
    console.print(Panel(Align.center(Text(title, style="bold blue on white"), vertical="middle"), height=3, border_style="blue"))


def run_validation():
    """Führt alle Validierungsabfragen im ClickHouse DWH mit Rich-Ausgabe aus."""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=CLICKHOUSE_DATABASE
        )
        console.print(Panel("[bold green]✅ Erfolgreich mit ClickHouse verbunden![/bold green]"))

        # --- DEMO SCHRITT 1: NACHWEIS VON SCD TYP 2 ---
        print_step_header("Schritt 1: Historisierung der Dimensionen (SCD Typ 2)")
        
        # NEU: Diagnose-Schritt, um zu prüfen, was wirklich in der DB ist
        console.print(f"\n[bold]Diagnose für Produkt-ID [yellow]{PRODUCT_ID_TO_VALIDATE}[/yellow]:[/bold]")
        diag_query = f"""
        SELECT 
            count() as total_versions,
            min({VALID_FROM_COL}) as earliest_entry
        FROM {DIM_PRODUCTS_TABLE}
        WHERE {PRODUCT_NK_COL} = %(product_id)s
        """
        params = {'product_id': PRODUCT_ID_TO_VALIDATE}
        diag_df = client.query_df(diag_query, parameters=params)
        console.print(create_rich_table(diag_df, "Diagnose-Ergebnis", border_style="magenta"))
        print_finding(f"Diese Abfrage zeigt das absolut früheste Datum, das für dieses Produkt in der DB gespeichert ist. Wenn dies nicht 1970 ist, liegt das Problem in der Daten-Pipeline.", emoji="💡")

        # Die ursprüngliche Abfrage, um alle Versionen zu zeigen
        scd2_query = f"""
        SELECT {PRODUCT_SK_COL}, {PRODUCT_NK_COL}, {PRODUCT_NAME_COL}, {AISLE_NAME_COL}, {DEPARTMENT_NAME_COL}, {VALID_FROM_COL}, {VALID_TO_COL}
        FROM {DIM_PRODUCTS_TABLE}
        WHERE {PRODUCT_NK_COL} = %(product_id)s
        ORDER BY {VALID_FROM_COL} ASC
        """
        scd2_df = client.query_df(scd2_query, parameters=params)
        
        console.print(f"\n[bold]Vollständige Historie für Produkt-ID [yellow]{PRODUCT_ID_TO_VALIDATE}[/yellow]:[/bold]")
        console.print(create_rich_table(scd2_df, "Produkthistorie"))
        
        if len(scd2_df) < 2:
            console.print("[bold red]Fehler:[/bold red] Es wurden nicht mehrere Versionen des Produkts gefunden.")
            return

        old_record = scd2_df.iloc[0]
        new_record = scd2_df[scd2_df[VALID_TO_COL].isnull()].iloc[0]
        
        old_product_sk = old_record[PRODUCT_SK_COL]
        new_product_sk = new_record[PRODUCT_SK_COL]
        
        print_finding(f"Der älteste gefundene Eintrag ([cyan]{old_product_sk}[/cyan]) hat ein `valid_to`-Datum und ist [red]historisiert[/red].")
        print_finding(f"Der neueste Eintrag ([cyan]{new_product_sk}[/cyan]) hat bei `valid_to` den Wert `None` und ist [green]aktuell gültig[/green].")

        # --- DEMO SCHRITT 2: DER VORHER-NACHHER-VERGLEICH ---
        print_step_header("Schritt 2: Validierung der Fakten-Verknüpfung (Alt vs. Neu)")

        # -- TEIL A: EINE ALTE BESTELLUNG ANALYSIEREN --
        console.print(Panel("Teil A: Analyse einer alten Bestellung (Vor der Simulation)", style="bold yellow", expand=False))
        
        old_order_fact_query = f"SELECT {ORDER_ID_COL}, {ORDER_PRODUCT_SK_COL} FROM {FACT_ORDER_PRODUCTS_TABLE} WHERE {ORDER_PRODUCT_SK_COL} = %(sk)s LIMIT 1"
        old_fact_df = client.query_df(old_order_fact_query, parameters={'sk': old_product_sk})

        old_order_joined_query = f"SELECT f.{ORDER_ID_COL}, f.{ORDER_PRODUCT_SK_COL}, p.{PRODUCT_NAME_COL} FROM {FACT_ORDER_PRODUCTS_TABLE} f JOIN {DIM_PRODUCTS_TABLE} p ON f.{ORDER_PRODUCT_SK_COL} = p.{PRODUCT_SK_COL} WHERE f.{ORDER_PRODUCT_SK_COL} = %(sk)s LIMIT 1"
        old_joined_df = client.query_df(old_order_joined_query, parameters={'sk': old_product_sk})

        old_fact_table = create_rich_table(old_fact_df, f"Rohdaten aus `{FACT_ORDER_PRODUCTS_TABLE}`", border_style="yellow")
        old_joined_table = create_rich_table(old_joined_df, "Verknüpfte Ansicht (JOIN)", border_style="yellow")
        console.print(Columns([old_fact_table, old_joined_table]))
        print_finding(f"Alte Bestellungen nutzen den alten Surrogate Key ([cyan]{old_product_sk}[/cyan]) und zeigen den [red]alten[/red] Produktnamen an.")

        # -- TEIL B: EINE NEUE BESTELLUNG ANALYSIEREN --
        console.print(Panel("Teil B: Analyse einer neuen Bestellung (Nach der Simulation)", style="bold green", expand=False))

        new_order_fact_query = f"SELECT {ORDER_ID_COL}, {ORDER_PRODUCT_SK_COL} FROM {FACT_ORDER_PRODUCTS_TABLE} WHERE {ORDER_PRODUCT_SK_COL} = %(sk)s LIMIT 1"
        new_fact_df = client.query_df(new_order_fact_query, parameters={'sk': new_product_sk})

        new_order_joined_query = f"SELECT f.{ORDER_ID_COL}, f.{ORDER_PRODUCT_SK_COL}, p.{PRODUCT_NAME_COL} FROM {FACT_ORDER_PRODUCTS_TABLE} f JOIN {DIM_PRODUCTS_TABLE} p ON f.{ORDER_PRODUCT_SK_COL} = p.{PRODUCT_SK_COL} WHERE f.{ORDER_PRODUCT_SK_COL} = %(sk)s LIMIT 1"
        new_joined_df = client.query_df(new_order_joined_query, parameters={'sk': new_product_sk})

        new_fact_table = create_rich_table(new_fact_df, f"Rohdaten aus `{FACT_ORDER_PRODUCTS_TABLE}`", border_style="green")
        new_joined_table = create_rich_table(new_joined_df, "Verknüpfte Ansicht (JOIN)", border_style="green")
        console.print(Columns([new_fact_table, new_joined_table]))
        print_finding(f"Neue Bestellungen nutzen den neuen Surrogate Key ([cyan]{new_product_sk}[/cyan]) und zeigen den [green]neuen[/green] Produktnamen an.")
        
        console.print(Panel("[bold green]🚀 DEMO-VALIDIERUNG ERFOLGREICH ABGESCHLOSSEN 🚀[/bold green]"))

    except Exception:
        console.print("\n[bold red]❌ EIN FEHLER IST AUFGETRETEN[/bold red]")
        console.print_exception(show_locals=False)

if __name__ == "__main__":
    # Bevor Sie das Skript ausführen, stellen Sie sicher, dass 'rich' installiert ist:
    # pip install rich
    run_validation()
