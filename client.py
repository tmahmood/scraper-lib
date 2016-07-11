from multiprocessing import Process
from libs.config import Config
import logging
from libs.message import Message


class Client(Process):
    """docstring for Client"""
    def __init__(self):
        super(Client, self).__init__()
        self.conn = None
        self.request_provider = None
        self.request_cleanup = None
        self.request = None
        cfg = Config()
        lcfg = '{}.client'.format(cfg.g('logger.base'))
        self.logger = logging.getLogger(lcfg)

    def set_conn(self, conn):
        """set listener connection"""
        self.conn = conn
        return self

    def set_request_provider(self, request_provider):
        """request handler"""
        self.request_provider = request_provider
        return self

    def set_request_cleanup(self, request_cleanup):
        """request_cleanup function"""
        self.request_cleanup = request_cleanup
        return self

    def set_daemon(self, mode):
        """should be daemon or not"""
        self.daemon = mode
        return self

    def run(self):
        data = self.conn.recv(2048)
        data = data.strip().decode('utf-8')
        self.logger.debug('received: %s', data)
        self.request = self.request_provider(data, Message(self.conn))
        self.request.check_request()
        return self

    def cleanup(self):
        """cleans up when exiting
        :returns: @todo
        """
        self.logger.info("pausing scrapers")
        self.request_cleanup()
