import json


class Config(object):
    """manage configurations"""
    def __init__(self, cfgfile='cfg/config.json'):
        super(Config, self).__init__()
        self.cfgfile = cfgfile
        with open(self.cfgfile) as jsonfile:
            self.cfg = json.load(jsonfile)

    def get(self, *args, **kwargs):
        k = self.cfg
        for kw in args:
            k = k[kw]
        return k

    def g(self, ky, default=False):
        keys = ky.split('.')
        k = self.cfg
        for kwrd in keys:
            if kwrd not in k:
                return default
            k = k[kwrd]
        return k
