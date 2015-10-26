import json


class Config(object):
    """manage configurations"""
    def __init__(self, cfgfile = 'cfg/config.json'):
        super(Config, self).__init__()
        self.cfgfile = cfgfile
        self.cfg = json.load(open(self.cfgfile))

    def get(self, *args, **kwargs):
        k = self.cfg
        for kw in args:
            k = k[kw]
        return k

    def g(self, ky, default=False):
        keys = ky.split('.')
        k = self.cfg
        for kw in keys:
            if not kw in k:
                return default
            k = k[kw]
        return k
