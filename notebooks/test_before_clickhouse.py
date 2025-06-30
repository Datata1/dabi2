# capture_db_state_refactored.py
# Führen Sie dieses Skript VOR und NACH Ihrer Datensimulation aus, 
# um die Änderungen im Data Warehouse deutlich zu machen.

import clickhouse_connect
import pandas as pd
import os
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich.live import Live
from rich.spinner import Spinner
from rich.rule import Rule
from rich.columns import Columns
from rich.syntax import Syntax
from typing import Union
import time

# --- 1. Konfiguration: Globale Konstanten ---
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "devpassword")
CLICKHOUSE_DATABASE = "default_marts"

DIM_PRODUCTS_TABLE = "dim_products"
FACT_ORDERS_TABLE = "f_orders"
FACT_ORDER_PRODUCTS_TABLE = "f_order_lines"

PRODUCT_NK_COL = "product_id"
PRODUCT_NAME_COL = "product_name"
VALID_TO_COL = "dbt_valid_to"

PRODUCT_ID_TO_WATCH = 35708

# --- 2. Hauptklasse für die Zustandserfassung ---
class DatabaseStateCapturer:
    """
    Erfasst den Zustand der DWH-Tabellen und stellt ihn im coolen Stil dar.
    """
    def __init__(self):
        self.console = Console()
        self.client = None

    def _connect(self):
        """Stellt die Verbindung zur Datenbank her und zeigt einen Spinner."""
        spinner = Spinner("dots", text=Text(f"Verbinde mit ClickHouse an {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...", style="bold cyan"))
        with Live(spinner, console=self.console, transient=True, vertical_overflow="visible"):
            try:
                self.client = clickhouse_connect.get_client(
                    host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER,
                    password=CLICKHOUSE_PASSWORD, database=CLICKHOUSE_DATABASE
                )
                time.sleep(0.2)
            except Exception as e:
                self.console.print(Panel(f"[bold red]DB-Verbindung fehlgeschlagen:[/bold red] {e}", border_style="red", expand=False))
                raise
        
        self.console.print(Panel(
            Text("✅ Verbindung zu ClickHouse erfolgreich!", justify="center", style="bold bright_green"),
            border_style="green", expand=False
        ))

    def _run_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """Führt eine Abfrage aus und gibt ein DataFrame zurück."""
        return self.client.query_df(query, parameters=params)

    # NEU: Übernommene Layout-Funktion für die Side-by-Side-Ansicht
    def _build_compact_view(self, query: str, result_renderable, query_title: str, result_title: str) -> Group:
        """
        Erstellt eine kompakte, nebeneinander liegende Ansicht für SQL und Ergebnis.
        """
        header_text = Text(f"🔬 ANALYSIERE: {query_title}", style="bold white on #F92672", justify="center")

        query_panel = Panel(
            Syntax(query.strip(), "sql", theme="monokai", line_numbers=True, word_wrap=True),
            title="SQL Abfrage", border_style="blue", expand=False
        )
        result_panel = Panel(
            Align.center(result_renderable, vertical="top"),
            title=result_title, border_style="green", expand=False
        )
        
        main_content = Columns([query_panel, result_panel])

        return Group(
            Panel(header_text, border_style="blue", height=3),
            main_content
        )

    # GEÄNDERT: Gibt jetzt ein renderbares Objekt zurück, anstatt zu drucken
    def _create_rich_table(self, df: pd.DataFrame, title: str) -> Union[Table, Panel]:
        """Erstellt eine Rich-Tabelle oder ein Panel aus einem DataFrame."""
        if df.empty:
            return Panel(
                f"[yellow]Keine Daten für '{title}' gefunden.[/yellow]", 
                border_style="yellow", expand=False
            )

        table = Table(title=Text(title, style="bold cyan"), show_header=True, header_style="bold #F92672", border_style="dim cyan", expand=False)
        
        for column in df.columns:
            table.add_column(str(column), justify="left", style="white")
            
        for _, row in df.iterrows():
            table.add_row(*[str(item) for item in row])
            
        return table

    def _print_step_header(self, title: str):
        """Druckt eine große, zentrierte Überschrift für einen Schritt."""
        self.console.print("\n")
        self.console.print(Rule(f"[bold bright_white on blue] {title} [/bold bright_white on blue]", style="blue"))

    def capture_and_display_state(self):
        """Führt den gesamten Prozess der Zustandserfassung aus."""

        try:
            self._connect()

            # --- Schritt 1: Gesamtzahlen der Zeilen erfassen ---
            self._print_step_header("1. Aktuelle Größe der Kern-Tabellen")
            
            counts_query = f"""
            SELECT
                (SELECT count() FROM {DIM_PRODUCTS_TABLE}) AS total_products,
                (SELECT count() FROM {FACT_ORDERS_TABLE}) AS total_orders,
                (SELECT count() FROM {FACT_ORDER_PRODUCTS_TABLE}) AS total_order_lines
            """
            counts_df = self._run_query(counts_query)
            # GEÄNDERT: Layout erstellen und ausgeben
            counts_table = self._create_rich_table(counts_df, "Tabellengrößen")
            counts_view = self._build_compact_view(counts_query, counts_table, "Gesamtzählung", "Ergebnis")
            self.console.print(counts_view)


            # --- Schritt 2: Den Zustand eines spezifischen Datensatzes erfassen ---
            self._print_step_header(f"2. Aktueller Zustand von Produkt-ID {PRODUCT_ID_TO_WATCH}")

            product_state_query = f"""
            SELECT {PRODUCT_NK_COL}, {PRODUCT_NAME_COL}, {VALID_TO_COL}
            FROM {DIM_PRODUCTS_TABLE}
            WHERE {PRODUCT_NK_COL} = %(product_id)s AND {VALID_TO_COL} IS NULL
            """
            params = {'product_id': PRODUCT_ID_TO_WATCH}
            product_df = self._run_query(product_state_query, params=params)
            
            # GEÄNDERT: Layout erstellen und ausgeben
            product_table = self._create_rich_table(product_df, f"Aktuell gültiger Eintrag")
            product_view = self._build_compact_view(
                product_state_query, 
                product_table, 
                f"Zustand von Produkt {PRODUCT_ID_TO_WATCH}", 
                "Ergebnis"
            )
            self.console.print(product_view)

        except Exception:
            self.console.print("\n[bold red]❌ EIN FEHLER IST AUFGETRETEN[/bold red]")
            self.console.print_exception(show_locals=False, width=self.console.width)

# --- 3. Ausführung ---
if __name__ == "__main__":
    capturer = DatabaseStateCapturer()
    capturer.capture_and_display_state()