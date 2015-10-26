import logging
from libs import listener
import sys
from libs import utils
from libs.sqlite import SQLite

logger = utils.setup_logger()

class Server(object):
    """docstring for Server"""
    def __init__(self, host, port):
        super(Server, self).__init__()
        self.host = host
        self.port = port
        self.db = SQLite()


    def start(self):
        try:
            conn = listener.ConnectionThread(self.host, self.port, self.db)
            utils.save_to_file('port', '{}'.format(self.port))
            conn.start()
        except Exception as e:
            print ("FAILED")
            print (e)
            try:
                if conn: conn.s.close()
            except Exception as e:
                pass

