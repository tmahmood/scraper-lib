from multiprocessing import Process
from libs.config import Config
import logging


class Client(Process):
    """docstring for Client"""
    def __init__(self):
        super(Client, self).__init__()
        self.request_provider = None
        self.request_cleanup = None
        self.request = None
        self.providerdata = None
        cfg = Config()
        lcfg = '{}.client'.format(cfg.g('logger.base'))
        self.logger = logging.getLogger(lcfg)

    def set_request_provider(self, request_provider):
        """request handler"""
        self.request_provider = request_provider
        return self

    def set_request_cleanup(self, request_cleanup):
        """request_cleanup function"""
        self.request_cleanup = request_cleanup
        return self

    def set_provider_data(self, providerdata):
        """set data

        :providerdata: @todo
        :returns: @todo

        """
        self.providerdata = providerdata
        return self

    def set_daemon(self, mode):
        """should be daemon or not"""
        self.daemon = mode
        return self

    def run(self):
        request = self.request_provider(self.providerdata)
        request.setup_repo()
        request.start()
        return self

    def cleanup(self):
        """cleans up when exiting
        :returns: @todo
        """
        self.logger.info("cleaning up")
        self.request_cleanup()
