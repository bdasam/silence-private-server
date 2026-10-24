import logging
import socket
import threading
import time

from _thread import start_new_thread

from helpers.constants import *
from services.SilenceServerService import SilenceServerService

log = logging.getLogger(LOGGER_NAME)


def mask_imei(imei):
    """Last 4 digits only, for log lines about unknown vehicles."""
    return "*" * max(0, len(imei) - 4) + imei[-4:]


class SinglePortRouter(threading.Thread):
    """Accepts every vehicle on one TCP port and hands each connection to the
    SilenceServerService of the IMEI found in its $ASTRA login frame.

    The per-IMEI services keep their own state, MQTT topics and command
    queues exactly as in per-port mode; only the listening socket is shared.
    """

    def __init__(self, port, services, login_timeout=10):
        super().__init__(daemon=True)
        self.port = port
        self.services = services            # {IMEI: SilenceServerService}
        self.login_timeout = login_timeout
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def route(self, conn):
        """Read the login frames, then dispatch. Returns the IMEI served, or None."""
        try:
            messages, crashed = SilenceServerService._telegramReceiver(
                None, conn, self.login_timeout, 0.2, "A", True)
            imei = SilenceServerService.extract_imei(messages) if not crashed else ""
            service = self.services.get(imei)
            if service is None:
                if imei:
                    log.warning(f"login from unknown IMEI {mask_imei(imei)}, closing")
                else:
                    log.warning("connection without a valid login frame, closing")
                conn.close()
                return None
            log.info(f"routing login to IMEI service {imei}")
            service.handle_client(conn, messages)
            return imei
        except Exception:
            log.exception("Exception routing vehicle connection")
            try:
                conn.close()
            except Exception:
                pass
            return None

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", self.port))
        s.settimeout(1)
        s.listen()
        log.info(f"single-port router listening on {self.port} for {len(self.services)} IMEIs")

        while not self._stop_event.is_set():
            try:
                conn, addr = s.accept()
            except socket.timeout:
                continue
            except Exception:
                log.exception("accept failed")
                time.sleep(0.1)
                continue
            log.info("client " + str(addr) + " connected")
            start_new_thread(self.route, (conn,))

        s.close()
