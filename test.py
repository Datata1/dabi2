import pandas as pd

# Passe den Pfad zur Parquet-Datei an
# Beispiel: Wenn die Datei im 'data/dwh_export/parquet' Ordner liegt
# und dein Notebook in 'notebooks/' ist:
parquet_file_path = './test3.parquet'
# Oder der Pfad im Container, wenn du ein Skript im Container ausführst:
# parquet_file_path = '/app/data/dwh_export/parquet/f_order_lines.parquet'

try:
    # Lese die Parquet-Datei in einen Pandas DataFrame
    df = pd.read_parquet(parquet_file_path)

    # Zeige die ersten paar Zeilen an
    print("--- Erste 5 Zeilen ---")
    print(df)

    # Zeige Informationen über die Spalten und Datentypen an
    print("\n--- Datei-Info ---")
    print(df.info())

    # Zeige grundlegende Statistiken für numerische Spalten an
    print("\n--- Statistik (numerisch) ---")
    print(df.describe())

    # Zeige die Anzahl der Zeilen an
    print(f"\nGesamte Zeilenanzahl: {len(df)}")

except FileNotFoundError:
    print(f"FEHLER: Datei nicht gefunden unter: {parquet_file_path}")
except Exception as e:
    print(f"FEHLER beim Lesen der Parquet-Datei: {e}")