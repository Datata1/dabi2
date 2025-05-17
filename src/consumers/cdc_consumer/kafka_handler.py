import json
from confluent_kafka import Consumer, KafkaError, KafkaException

from . import config 
from .config import get_logger 

logger = get_logger(__name__)

def create_kafka_consumer() -> Consumer | None:
    """
    Erstellt und konfiguriert einen Kafka Consumer.
    Gibt den Consumer zurück oder None bei einem Fehler.
    """
    conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': config.CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  
    }
    try:
        consumer = Consumer(conf)
        consumer.subscribe([config.KAFKA_TOPIC_PATTERN])
        logger.info(f"Kafka Consumer erstellt und auf Topic-Pattern '{config.KAFKA_TOPIC_PATTERN}' subscribed.")
        return consumer
    except KafkaException as e:
        logger.error(f"Kritischer Fehler bei der Kafka Consumer Initialisierung: {e}", exc_info=True)
        return None

def consume_message(consumer: Consumer, message_buffer: dict) -> bool:
    """
    Konsumiert eine einzelne Nachricht von Kafka.
    Dekodiert die Nachricht und fügt das Payload dem message_buffer hinzu.
    """
    msg = consumer.poll(timeout=config.KAFKA_POLL_TIMEOUT)

    if msg is None:
        return False  # Timeout, keine neue Nachricht

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # logger.debug(f"Ende der Partition erreicht für {msg.topic()} [{msg.partition()}] bei Offset {msg.offset()}")
            return False
        else:
            logger.error(f"Kafka Error: {msg.error()}", exc_info=True)
            raise KafkaException(msg.error()) 

    # Erfolgreiche Nachricht
    try:
        value_str = msg.value().decode('utf-8')
        data = json.loads(value_str)

        # Debezium-spezifische Payload-Extraktion
        payload = data.get('payload', {}) 

        # Falls 'payload' null ist oder kein Dictionary ist, die gesamte Nachricht als Fallback verwenden
        if payload is None or not isinstance(payload, dict):
            if payload is None: 
                logger.warning(f"Nachricht für Topic '{msg.topic()}' hat 'payload: null'. Verwende leeres Payload.")
                payload = {} 
            else: 
                logger.warning(f"Nachricht für Topic '{msg.topic()}' hat unerwarteten Payload-Typ: {type(payload)}. Wert: {payload}. Verwende die gesamte Nachricht als Payload.")
                payload = data 
                if not isinstance(payload, dict): 
                    logger.error(f"Selbst die gesamte Nachricht für Topic '{msg.topic()}' ist kein Dictionary: {type(payload)}. Überspringe Nachricht.")
                    return False 


        topic_name = msg.topic()
        message_buffer[topic_name].append(payload)
        # logger.debug(f"Nachricht zu Puffer für Topic {topic_name} hinzugefügt. Puffergröße: {len(message_buffer[topic_name])}")
        return True
    except json.JSONDecodeError:
        logger.error(f"Fehler beim JSON-Dekodieren der Nachricht von Topic '{msg.topic()}': {msg.value()}", exc_info=True)
        # Hier könnte man entscheiden, die Nachricht zu überspringen oder den Fehler weiterzuleiten
        raise 
    except UnicodeDecodeError:
        logger.error(f"Fehler beim UTF-8 Dekodieren der Nachricht von Topic '{msg.topic()}'.", exc_info=True)
        raise 
    except Exception as e:
        logger.error(f"Unerwarteter Fehler bei der Nachrichtenverarbeitung von Topic '{msg.topic()}': {e}", exc_info=True)
        raise 


def commit_offsets(consumer: Consumer) -> bool:
    """
    Führt ein synchrones Commit der aktuellen Offsets durch.
    """
    try:
        consumer.commit(asynchronous=config.KAFKA_COMMIT_ASYNCHRONOUS)
        logger.info("Kafka Offsets erfolgreich committed.")
        return True
    except KafkaException as e:
        logger.error(f"Fehler beim Committen der Kafka Offsets: {e}", exc_info=True)
        return False