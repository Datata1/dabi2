import sys

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from cdc_consumer import main 
from cdc_consumer.config import get_logger 

runner_logger = get_logger("CdcConsumerRunner")

if __name__ == "__main__":
    try:
        main.run() 
    except SystemExit as e:
        runner_logger.info(f"Anwendung beendet mit Exit Code {e.code}.")
        sys.exit(e.code) 
    except KeyboardInterrupt:
        runner_logger.info("Anwendung durch Benutzer (KeyboardInterrupt) unterbrochen. Beende sauber...")
        sys.exit(0) 
    except Exception as e:
        runner_logger.critical(f"Ein unerwarteter, nicht abgefangener Fehler ist im Hauptprogramm aufgetreten: {e}", exc_info=True)
        sys.exit(1) 