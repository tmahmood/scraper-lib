"""
multiprocessing server
"""
from libs import utils
from time import sleep

LOGGER = utils.setup_logger()


class Server(object):
    """docstring for ServerMultiProcess"""

    def __init__(self):
        super(Server, self).__init__()
        self.client_provider = None
        self.provider = None

    def set_client_provider(self, client_provider):
        """request handler"""
        self.client_provider = client_provider
        return self

    def set_provider(self, provider):
        """data provider

        :provider: @todo
        :returns: @todo

        """
        self.provider = provider
        return self

    def start(self):
        """everything starts here"""
        process = None
        try:
            while True:
                try:
                    providerdata = self.provider.get_queued_data()
                    if providerdata is not None:
                        process = self.client_provider()\
                                      .set_provider_data(providerdata)
                        process.start()
                    sleep(3)
                except KeyboardInterrupt:
                    break
                except Exception:
                    LOGGER.exception("FAILED")
                    break
        except Exception:
            LOGGER.exception("server out ...")
        finally:
            LOGGER.info('cleaning up')
            # set running scrapers to be paused
            if process != None:
                process.cleanup()
