from libs.config import Config
import logging
from libs.message import Message
import asyncore


class AsyncHandler(asyncore.dispatcher_with_send):

    def __init__(self, sock, request_provider):
        self.request_provider = request_provider
        cfg = Config()
        lcfg = '{}.client'.format(cfg.g('logger.base'))
        self.logger = logging.getLogger(lcfg)
        asyncore.dispatcher_with_send.__init__(self, sock)

    def handle_read(self):
        """reads data from socket"""
        data = self.recv(2048)
        data = data.strip().decode('utf-8')
        self.logger.debug('received: %s', data)
        self.request = self.request_provider(data, Message(self))
        self.request.check_request()
