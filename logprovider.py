import logging


class LogProvider(object):
    """make loggers"""
    def __init__(self):
        super(LogProvider, self).__init__()

    def get(self, config, suffix):
        """get logger with name

        :config: config class
        :suffix: additional text
        :returns: @todo

        """
        txt = '{}.{}'.format(config.g('logger.base'), suffix)
        return logging.getLogger(txt)

