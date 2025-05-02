#!/bin/sh
# Exit sofort bei Fehlern
set -e

CONNECTOR_NAME="oltp-postgres-connector"
# Wichtig: Internen Service-Namen verwenden!
CONNECT_URL="http://kafka-connect:8083/connectors"
CONFIG_FILE="/config/debezium-pg-connector.json"

echo "Provisioner: Warte auf Kafka Connect unter $CONNECT_URL..."

# Einfache Schleife, die wartet, bis die /connectors API antwortet
# In Produktion wären robustere Checks (z.B. mit jq) besser
until curl -s -f $CONNECT_URL > /dev/null; do
  echo "Provisioner: Kafka Connect ($CONNECT_URL) noch nicht bereit, warte 5s..."
  sleep 5
done

echo "Provisioner: Kafka Connect ist bereit."

# Prüfe, ob der Connector bereits existiert
echo "Provisioner: Prüfe auf existierenden Connector '$CONNECTOR_NAME'..."
STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" $CONNECT_URL/$CONNECTOR_NAME)

if [ "$STATUS_CODE" -eq 200 ]; then
  echo "Provisioner: Connector '$CONNECTOR_NAME' existiert bereits. Keine Aktion nötig."
  # Optional: Konfiguration aktualisieren mit PUT (siehe vorherige Antwort)
elif [ "$STATUS_CODE" -eq 404 ]; then
  echo "Provisioner: Connector '$CONNECTOR_NAME' existiert nicht. Erstelle ihn..."
  # Erstelle den Connector mit der Konfigurationsdatei
  curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  $CONNECT_URL -d @$CONFIG_FILE
  # Optional: Prüfe den HTTP-Status des POST-Befehls hier
  echo "Provisioner: Anfrage zum Erstellen von '$CONNECTOR_NAME' gesendet."
else
  echo "Provisioner: Fehler beim Prüfen des Connector-Status (HTTP Code: $STATUS_CODE). Abbruch."
  exit 1
fi

echo "Provisioner: Debezium Provisionierung abgeschlossen."
exit 0