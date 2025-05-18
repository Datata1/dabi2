import pandas as pd

# Pfad zur Parquet-Datei
parquet_datei_pfad = 'test.parquet'

try:
    # Parquet-Datei in ein Pandas DataFrame einlesen
    df = pd.read_parquet(parquet_datei_pfad)

    # Die ersten 5 Zeilen des DataFrames ausgeben
    print(df.head())

except FileNotFoundError:
    print(f"Fehler: Die Datei '{parquet_datei_pfad}' wurde nicht gefunden.")
except Exception as e:
    print(f"Ein Fehler ist aufgetreten: {e}")