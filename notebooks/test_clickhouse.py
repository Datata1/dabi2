# test_clickhouse_validation_final.py

import clickhouse_connect
import pandas as pd
import os
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich.padding import Padding
from rich.syntax import Syntax
from rich.live import Live
from rich.spinner import Spinner
from rich.rule import Rule
from rich.columns import Columns # NEU: Importiert für kompaktes Layout
import time

# --- 1. Konfiguration: Globale Konstanten ---
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "devpassword")
CLICKHOUSE_DATABASE = "default_marts"

DIM_PRODUCTS_TABLE = "dim_products"
FACT_ORDER_PRODUCTS_TABLE = "f_order_lines"
PRODUCT_SK_COL, PRODUCT_NK_COL, PRODUCT_NAME_COL = "dbt_scd_id", "product_id", "product_name"
AISLE_NAME_COL, DEPARTMENT_NAME_COL = "aisle_name", "department_name"
VALID_FROM_COL, VALID_TO_COL = "dbt_valid_from", "dbt_valid_to"
ORDER_ID_COL, ORDER_PRODUCT_SK_COL, ORDER_TIMESTAMP_COL = "order_id", "product_sk", "order_timestamp"

PRODUCT_ID_TO_VALIDATE = 35708

# --- 2. Hauptklasse für die Validierung ---
class ClickHouseValidator:
    """
    Eine Klasse zur Durchführung und coolen Darstellung von SCD Typ 2 Validierungen in ClickHouse.
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
                time.sleep(1)
            except Exception as e:
                self.console.print(Panel(f"[bold red]DB-Verbindung fehlgeschlagen:[/bold red] {e}", border_style="red"))
                raise

        self.console.print(Panel(
            Text("🚀 Verbindung zu ClickHouse DWH erfolgreich hergestellt!", justify="center", style="bold bright_green"),
            border_style="green", expand=False
        ))
        time.sleep(1)

    def _format_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Formatiert Unix-Timestamps (microseconds) in ein lesbares Format."""
        if ORDER_TIMESTAMP_COL in df.columns:
            df = df.copy()
            df[ORDER_TIMESTAMP_COL] = pd.to_datetime(df[ORDER_TIMESTAMP_COL], unit='us').dt.strftime('%Y-%m-%d %H:%M:%S')
        return df

    def _run_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """Führt eine Abfrage aus und gibt ein DataFrame zurück."""
        return self.client.query_df(query, parameters=params)

    # --- KORRIGIERT: Diese Funktion ersetzt _build_layout ---
    def _build_compact_view(self, query: str, result_renderable, query_title: str, result_title: str) -> Group:
        """
        Erstellt eine kompakte, nebeneinander liegende Ansicht ohne leeren Raum.
        Verwendet Columns statt Layout für eine dynamische Höhe.
        """
        header_text = Text(f"🔬 ANALYSIERE: {query_title}", style="bold white on #F92672", justify="center")

        query_panel = Panel(
            Syntax(query, "sql", theme="monokai", line_numbers=True, word_wrap=True),
            title="SQL Abfrage",
            border_style="blue",
            expand=False # Wichtig: Panel nicht expandieren
        )
        result_panel = Panel(
            Align.center(result_renderable, vertical="top"), # 'top' Ausrichtung für besseren Look
            title=result_title,
            border_style="green",
            expand=False # Wichtig: Panel nicht expandieren
        )

        # Columns richtet die Panels nebeneinander aus, Höhe passt sich an
        main_content = Columns([query_panel, result_panel])

        # Group fasst Header und Inhalt vertikal zusammen
        return Group(
            Panel(header_text, border_style="blue", height=3),
            main_content
        )

    def _create_rich_table(self, df: pd.DataFrame) -> Table:
        """
        Erstellt eine Rich-Tabelle, die kompakt ist und maximal 4 Zeilen anzeigt.
        """
        table = Table(show_header=True, header_style="bold #F92672", border_style="dim cyan", expand=False)
        for column in df.columns:
            style = "bold cyan" if "_sk" in column or "_id" in column else "white"
            table.add_column(str(column), justify="left", style=style, no_wrap=True)

        def add_styled_row(row_data):
            row_values = [str(item) for item in row_data]
            row_style = ""
            if PRODUCT_NAME_COL in df.columns and "TEST" in str(row_data.get(PRODUCT_NAME_COL, "")):
                row_style = "bold bright_green"
            table.add_row(*row_values, style=row_style)

        if len(df) > 8:
            for _, row in df.head(4).iterrows():
                add_styled_row(row)
            table.add_row(*["..."] * len(df.columns), style="dim")
            for _, row in df.tail(4).iterrows():
                add_styled_row(row)
        else:
            for _, row in df.iterrows():
                add_styled_row(row)
        return table

    def _print_finding(self, text: str, emoji="💡"):
        """Druckt eine wichtige Erkenntnis formatiert."""
        self.console.print(Padding(f"[bold #F92672]{emoji} ERKENNTNIS:[/bold #F92672] {text}", (1, 0, 1, 4)))

    def _print_step_header(self, title: str):
        """Druckt eine große, zentrierte Überschrift für einen Demo-Schritt."""
        self.console.print("\n")
        self.console.print(Rule(f"[bold bright_white on blue] {title} [/bold bright_white on blue]", style="blue"))
        self.console.print("\n")

    def run_validation(self):
        """Führt den gesamten Validierungsprozess aus."""
        try:
            self._connect()

            # --- SCHRITT 1: NACHWEIS VON SCD TYP 2 ---
            self._print_step_header("Schritt 1: Historisierung der Dimensionen (SCD Typ 2)")
            params = {'product_id': PRODUCT_ID_TO_VALIDATE}
            scd2_query = f"""
SELECT
    {PRODUCT_SK_COL}, {PRODUCT_NK_COL}, {PRODUCT_NAME_COL}, {AISLE_NAME_COL},
    {VALID_FROM_COL}, {VALID_TO_COL}
FROM {DIM_PRODUCTS_TABLE}
WHERE {PRODUCT_NK_COL} = %(product_id)s
ORDER BY {VALID_FROM_COL} ASC
"""
            scd2_df = self._run_query(scd2_query, params=params)
            
            # KORRIGIERT: Aufruf der neuen Funktion
            compact_view = self._build_compact_view(scd2_query, self._create_rich_table(scd2_df), "Produkthistorie", "Versionen des Produkts")
            self.console.print(compact_view)

            if len(scd2_df) < 2:
                self.console.print("[bold red]Validierung fehlgeschlagen:[/bold red] Es wurden nicht mehrere Versionen des Produkts gefunden.")
                return

            old_record = scd2_df.iloc[0]
            new_record = scd2_df[scd2_df[VALID_TO_COL].isnull()].iloc[0]
            old_product_sk, new_product_sk = old_record[PRODUCT_SK_COL], new_record[PRODUCT_SK_COL]

            self._print_finding(f"Der älteste Eintrag ([cyan]{old_product_sk}[/cyan]) hat ein `valid_to`-Datum und ist [red]historisiert[/red].")
            self._print_finding(f"Der neueste Eintrag ([cyan]{new_product_sk}[/cyan]) hat kein `valid_to`-Datum und ist [bright_green]aktuell gültig[/bright_green].")

            # --- SCHRITT 2: DER VORHER-NACHHER-VERGLEICH ---
            self._print_step_header("Schritt 2: Validierung der Fakten-Verknüpfung (Alt vs. Neu)")
            self._validate_fact_linkage("ALT", old_product_sk)
            self._validate_fact_linkage("NEU", new_product_sk)

            self.console.print(Panel(
                Text("✅🚀 VALIDIERUNG ERFOLGREICH ABGESCHLOSSEN 🚀✅", justify="center", style="bold white on green"),
                padding=(1, 4), border_style="green"
            ))

        except Exception:
            self.console.print("\n[bold red]❌ EIN FEHLER IST AUFGETRETEN[/bold red]")
            self.console.print_exception(show_locals=False, width=self.console.width)

    def _validate_fact_linkage(self, version_label: str, product_sk: str):
        """
        Analysiert eine Bestellung für einen gegebenen Surrogate Key und stellt das Ergebnis dar.
        """
        color = "yellow" if version_label == "ALT" else "green"
        panel_title = f"Teil {version_label}: Analyse einer {'alten' if version_label == 'ALT' else 'neuen'} Bestellung"
        self.console.print(Panel(Text(panel_title, justify="center"), style=f"bold {color}", border_style=color))

        params = {'sk': product_sk}
        find_order_query = f"""
SELECT {ORDER_ID_COL} FROM {FACT_ORDER_PRODUCTS_TABLE}
WHERE {ORDER_PRODUCT_SK_COL} = %(sk)s
ORDER BY {ORDER_TIMESTAMP_COL} DESC
LIMIT 1
"""
        target_order_df = self._run_query(find_order_query, params=params)

        if target_order_df.empty:
            self.console.print(Panel(f"[yellow]Keine passende Bestellung für SK [cyan]{product_sk}[/cyan] gefunden.[/yellow]"))
            return

        target_order_id = target_order_df.iloc[0][ORDER_ID_COL]
        params_order = {'sk': product_sk, 'order_id': target_order_id}

        joined_query = f"""
SELECT
    f.{ORDER_ID_COL}, f.{ORDER_PRODUCT_SK_COL}, p.{PRODUCT_NAME_COL}, f.{ORDER_TIMESTAMP_COL}
FROM {FACT_ORDER_PRODUCTS_TABLE} AS f
JOIN {DIM_PRODUCTS_TABLE} AS p
    ON f.{ORDER_PRODUCT_SK_COL} = p.{PRODUCT_SK_COL}
WHERE f.{ORDER_PRODUCT_SK_COL} = %(sk)s AND f.{ORDER_ID_COL} = %(order_id)s
"""
        joined_df = self._run_query(joined_query, params=params_order)
        joined_df = self._format_timestamps(joined_df)

        # KORRIGIERT: Aufruf der neuen Funktion
        compact_view = self._build_compact_view(joined_query, self._create_rich_table(joined_df), f"Verknüpfte Daten ({version_label})", "JOIN-Ergebnis")
        self.console.print(compact_view)

        if not joined_df.empty:
            timestamp_str = joined_df.iloc[0][ORDER_TIMESTAMP_COL]
            product_name_style = f"[red]{version_label}EN[/red]" if version_label == "ALT" else f"[bright_green]{version_label}EN[/bright_green]"
            self._print_finding(f"{version_label.capitalize()}e Bestellungen (z.B. vom [magenta]{timestamp_str}[/magenta]) nutzen den SK [cyan]{product_sk}[/cyan] und zeigen den {product_name_style} Produktnamen.")

# --- 3. Ausführung ---
if __name__ == "__main__":
    validator = ClickHouseValidator()
    validator.run_validation()