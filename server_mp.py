"""
multiprocessing server
"""
import socket
from inc.client import Client
from libs import utils

LOGGER = utils.setup_logger()


class Server(object):
    """docstring for ServerMultiProcess"""

    def __init__(self, host, port):
        super(Server, self).__init__()
        self.host = host
        self.port = int(port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(10)
        utils.save_to_file('port', '{}'.format(self.port))

    def start(self):
        """everything starts here

        """
        process = None
        try:
            while True:
                try:
                    conn, address = self.socket.accept()
                    LOGGER.info("Got connection %s", address)
                    process = Client(conn)
                    process.daemon = True
                    process.start()
                except KeyboardInterrupt:
                    if conn:
                        conn.close()
                    break
                except Exception:
                    LOGGER.exception("FAILED")
                    if conn:
                        conn.close()
                    break
        except Exception:
            LOGGER.exception("server out ...")
        finally:
            LOGGER.info('cleaning up')
            # set running scrapers to be paused
            if process != None:
                process.cleanup()
            self.socket.close()
