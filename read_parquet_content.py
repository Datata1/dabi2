import pandas as pd
import argparse
import sys
import logging
from pathlib import Path

# --- Logging Konfiguration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

def read_local_parquet_content(file_path_str: str, limit: int = 10):
    """
    Liest den Inhalt einer lokalen Parquet-Datei mit Pandas und gibt ihn aus.
    """
    logger.info(f"Versuche Inhalt zu lesen von lokaler Datei: {file_path_str}")
    if limit > 0:
        logger.info(f"Zeige maximal {limit} Zeilen.")
    else:
        logger.info("Zeige alle Zeilen (Limit=0).")

    file_path = Path(file_path_str).resolve() # Stelle sicher, dass der Pfad absolut ist

    # Prüfen, ob Datei existiert
    if not file_path.is_file():
        logger.error(f"FEHLER: Datei nicht gefunden unter: {file_path}")
        return False

    try:
        # Lese die Parquet-Datei mit Pandas (benötigt 'pyarrow' oder 'fastparquet')
        logger.debug(f"Lese Parquet-Datei mit pandas: {file_path}")
        df = pd.read_parquet(file_path, engine='pyarrow') # Oder engine='fastparquet'

        print("\n" + "=" * 80)
        print(f"Inhalt von: {file_path}")
        print("-" * 80)
        print(f"Schema (Spalten, Nicht-Null-Werte, Datentypen von Pandas erkannt):")
        # Zeige Spalten, Non-Null Counts und Dtypes
        # buffer wird benötigt, um info() in einen String zu bekommen für logger
        from io import StringIO
        buffer = StringIO()
        df.info(buf=buffer, verbose=True, show_counts=True)
        logger.info("\n" + buffer.getvalue())
        print("-" * 80)
        print(f"Erste {limit if limit > 0 else 'alle'} Zeilen:")

        # Zeige die ersten 'limit' Zeilen (oder alle, wenn limit <= 0)
        # Passe Pandas Anzeigeoptionen für bessere Lesbarkeit in der Konsole an
        with pd.option_context(
            'display.max_rows', None if limit <= 0 else limit,
            'display.max_columns', None,       # Alle Spalten anzeigen
            'display.width', 1000,            # Breite der Anzeige erhöhen
            'display.max_colwidth', 50        # Maximale Breite einzelner Spalten begrenzen
            ):
             if limit > 0:
                print(df.head(limit))
             else:
                # Vorsicht bei sehr großen Dateien! Könnte viel Output erzeugen.
                print(df)

        print("=" * 80)
        return True

    except ImportError:
         logger.error("FEHLER: Pandas oder die Parquet Engine ('pyarrow' oder 'fastparquet') ist nicht installiert.")
         logger.error("Bitte installieren: pip install pandas pyarrow")
         return False
    except Exception as e:
        logger.error(f"\nFEHLER beim Lesen oder Verarbeiten der Parquet-Datei: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Liest den Inhalt einer lokalen Parquet-Datei.')
    parser.add_argument('local_file_path', type=str, help='Der vollständige lokale Pfad zur Parquet-Datei.')
    parser.add_argument('--limit', '-l', type=int, default=10, help='Maximale Anzahl der anzuzeigenden Zeilen (0 für alle). Default: 10.')
    args = parser.parse_args()

    # Setze Log-Level auf DEBUG für mehr Details, wenn gewünscht
    # logging.getLogger().setLevel(logging.DEBUG)

    if read_local_parquet_content(args.local_file_path, args.limit):
        print("\nSkript erfolgreich beendet.")
        sys.exit(0)
    else:
        print("\nSkript mit Fehlern beendet.")
        sys.exit(1)