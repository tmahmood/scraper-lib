from libs import listener
from libs import utils
from libs.mysql import MySQL

logger = utils.setup_logger()


class Server(object):
    """docstring for Server"""
    def __init__(self, host, port):
        super(Server, self).__init__()
        self.host = host
        self.port = int(port)
        self.db = MySQL()
        self.db.query('delete from runlogs')

    def start(self):
        try:
            conn = listener.ConnectionThread(self.host, self.port, self.db)
            utils.save_to_file('port', '{}'.format(self.port))
            conn.start()
        except Exception as e:
            print ("FAILED")
            print (e)
            try:
                if conn:
                    conn.s.close()
            except Exception as e:
                pass


