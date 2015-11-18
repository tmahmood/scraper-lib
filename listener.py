import logging
import socket
import threading
import sys
import os
from libs.config import Config
from inc.client import Client

g_config = Config()
l = '{}.server'.format(g_config.g('logger.base'))
logger = logging.getLogger(l)


class ConnectionThread(threading.Thread):

    def __init__(self, host, port, db):
        super(ConnectionThread, self).__init__()
        self.db = db
        self.db.query('delete from runlogs')
        try:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.bind((host, port))
            logger.info('listening to %s:%s', host, port)
            self.s.listen(5)
        except socket.error:
            self.s.close()
            sys.exit()
        self.clients = []

    def main_loop(self):
        try:
            while True:
                if os.path.exists('cache/stop'):
                    break
                conn, address = self.s.accept()
                logger.info('[+] Client connected: {0}'.format(address[0]))
                c = Client(conn, self.db)
                c.start()
                self.clients.append(c)
        except Exception:
            logger.exception("Error!")
        finally:
            logger.info("[-] Closing connection")
            conn.close()
            os.remove('cache/stop')
            sys.exit()

    def run(self):
        self.main_loop()
