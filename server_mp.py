import socket
from inc.client import Client
from libs import utils
from libs.mysql import MySQL

logger = utils.setup_logger()


class Server(object):
    """docstring for ServerMultiProcess"""
    def __init__(self, host, port):
        super(Server, self).__init__()
        self.host = host
        self.port = int(port)
        self.db = MySQL()
        self.db.query('update scrapers set stage = 0')
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(10)
        utils.save_to_file('port', '{}'.format(self.port))

    def start(self):
        """everything starts here
        """
        while True:
            try:
                conn, address = self.socket.accept()
                logger.info("Got connection %s", address)
                process = Client(conn)
                process.daemon = True
                process.start()
            except KeyboardInterrupt:
                if conn:
                    conn.close()
                break
            except Exception:
                logger.exception("FAILED")
                if conn:
                    conn.close()
                break
        self.socket.close()
        self.db.query('update scrapers set stage = 0')
