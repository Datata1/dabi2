# CDC Kafka to MinIO Parquet Writer

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org)

Ein robuster und konfigurierbarer Python-basierter Kafka-Consumer, der Change Data Capture (CDC)-Nachrichten (z.B. von Debezium) von Kafka-Topics konsumiert, diese verarbeitet, im Batch-Verfahren sammelt und als Parquet-Dateien in einem MinIO Data Lake ablegt. Nach erfolgreichem Schreiben eines Batches kann optional ein Prefect DWH (Data Warehouse) Flow getriggert werden.

## Inhaltsverzeichnis

- [Funktionsweise](#funktionsweise)
- [Konfiguration](#konfiguration)
  - [Umgebungsvariablen](#umgebungsvariablen)
- [Datenstruktur in MinIO](#datenstruktur-in-minio)
- [Projektstruktur](#projektstruktur)


## Funktionsweise

Der Consumer führt folgende Schritte aus:

1.  **Konsumieren:** Lauscht auf Kafka-Topics, die einem definierten Regex-Pattern entsprechen.
2.  **Puffern:** Sammelt eingehende Nachrichten in einem internen Puffer, gruppiert nach Topic.
3.  **Transformieren:** Verarbeitet die JSON-Payloads der Nachrichten:
    * Extrahiert relevante CDC-Informationen (Operation, Zeitstempel).
    * Fügt Metadaten hinzu (Verarbeitungszeitpunkt, Kafka-Topic).
    * Konvertiert die Daten in einen Pandas DataFrame.
4.  **Batch-Schreiben nach MinIO:** In regelmäßigen Zeitintervallen (oder optional bei Erreichen einer maximalen Batch-Größe):
    * Konvertiert den DataFrame für jedes Topic in das Parquet-Format (mit Snappy-Kompression).
    * Schreibt die Parquet-Datei in den konfigurierten MinIO-Bucket. Die Daten werden dabei nach Tabelle, Jahr, Monat und Tag partitioniert.
5.  **Prefect Trigger:** Nach dem erfolgreichen Schreiben der Daten für ein Topic nach MinIO wird ein konfigurierter Prefect Flow getriggert, um nachgelagerte DWH-Prozesse anzustoßen.
6.  **Offset Commit:** Nach erfolgreicher Verarbeitung und Speicherung eines Batches werden die entsprechenden Kafka-Offsets synchron committet, um "At-least-once"-Semantik sicherzustellen.



## Konfiguration

Die Anwendung wird vollständig über Umgebungsvariablen konfiguriert.

### Umgebungsvariablen

| Variable                        | Beschreibung                                                                 | Standardwert                             | Erforderlich |
| :------------------------------ | :--------------------------------------------------------------------------- | :--------------------------------------- | :----------- |
| **Kafka** |                                                                              |                                          |              |
| `KAFKA_BOOTSTRAP_SERVERS`       | Komma-separierte Liste der Kafka Broker (host:port).                         | `kafka:29092`                            | Ja           |
| `KAFKA_TOPIC_PATTERN`           | Regex-Pattern für die zu konsumierenden Kafka-Topics.                      | `^cdc\.oltp_dabi\.public\.(.*)`          | Ja           |
| `CONSUMER_GROUP_ID`             | Group ID für den Kafka Consumer.                                             | `dabi2-minio-lake-writer`                | Ja           |
| `KAFKA_POLL_TIMEOUT`            | Timeout in Sekunden für den `consumer.poll()` Aufruf.                        | `1.0`                                    | Nein         |
| `KAFKA_COMMIT_ASYNCHRONOUS`     | Ob Kafka Offsets asynchron (`true`) oder synchron (`false`) committet werden. | `false` (empfohlen für Datensicherheit) | Nein         |
| **MinIO** |                                                                              |                                          |              |
| `MINIO_ENDPOINT`                | Endpoint des MinIO Servers (host:port).                                      | `minio:9000`                             | Ja           |
| `MINIO_ACCESS_KEY`              | Access Key für MinIO.                                                        | `minioadmin`                             | Ja           |
| `MINIO_SECRET_KEY`              | Secret Key für MinIO.                                                        |                                          | Ja           |
| `MINIO_BUCKET`                  | Name des MinIO Buckets, in den geschrieben wird.                             | `datalake`                               | Ja           |
| `MINIO_USE_SSL`                 | Ob SSL/TLS für die Verbindung zu MinIO verwendet werden soll (`true`/`false`). | `false`                                  | Nein         |
| **Batching** |                                                                              |                                          |              |
| `WRITE_INTERVAL_SECONDS`        | Intervall in Sekunden, in dem Batches nach MinIO geschrieben werden.         | `20`                                     | Nein         |
| **Prefect** |                                                                              |                                          |              |
| `PREFECT_API_URL`               | (Implizit von Prefect Client verwendet) URL der Prefect API.                 | (Prefect Default)                        | Nein         |
| `PREFECT_API_KEY`               | (Implizit von Prefect Client verwendet) API Key für Prefect Cloud.           | (Prefect Default)                        | Nein         |
| `PREFECT_FLOW_NAME`             | Name des Prefect Flows, der getriggert werden soll.                          | `cdc_minio_to_duckdb_flow`               | Nein         |
| `PREFECT_DEPLOYMENT_NAME`       | Name des Prefect Deployments, das getriggert werden soll.                    | `dwh-pipeline`                           | Nein         |
| **Logging** |                                                                              |                                          |              |
| `LOG_LEVEL`                     | Log-Level für die Anwendung (DEBUG, INFO, WARNING, ERROR, CRITICAL).         | `INFO`                                   | Nein         |

## Datenstruktur in MinIO

```
s3://<MINIO_BUCKET>/cdc_events/<table_name>/year=<YYYY>/month=<MM>/day=<DD>/<table_name>_<timestamp>.parquet
```

## Projektstruktur

```
cdc-kafka-minio-writer/
├── src/
│   └── cdc_consumer/           # Kernlogik der Anwendung
│       ├── __init__.py
│       ├── main.py             # Orchestrierung der Hauptschleife
│       ├── config.py           # Konfigurationsmanagement und Validierung
│       ├── kafka_handler.py      # Kafka-spezifische Funktionen
│       ├── message_processor.py  # Transformation und Aufbereitung der Nachrichten
│       ├── minio_handler.py      # MinIO-spezifische Funktionen
│       ├── prefect_handler.py    # Prefect-spezifische Funktionen
│       └── utils.py            # Hilfsfunktionen (z.B. GracefulKiller)
├── Dockerfile                  # Docker-Definition für den Container
├── pyproject.toml              # UV Konfiguration
├── main.py             # Startskript für die Anwendung
└── README.md                   
```