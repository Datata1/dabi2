import signal
import logging

from .config import get_logger
logger = get_logger(__name__)

class GracefulKiller:
    """
    Behandelt SIGINT und SIGTERM für ein sauberes Herunterfahren.
    Setzt ein Flag, das von der Hauptschleife überprüft werden kann.
    """
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        logger.info("GracefulKiller initialisiert. SIGINT und SIGTERM werden abgefangen.")

    def exit_gracefully(self, signum, frame):
        logger.info(f"Signal {signal.Signals(signum).name} empfangen. Beende Consumer...")
        self.kill_now = True