import re
import codecs
import logging
import config
import datetime
import json

try:
    from hashlib import md5
except ImportError as e:
    from md5 import md5

config = config.Config()


def read_file(filename, linewise=False):
    try:
        with open(filename) as fptr:
            content = fptr.read().strip()
            if linewise:
                content = content.split("\n")
        return content
    except Exception as e:
        print (filename)
        raise e


def save_to_file(filename, content, use_codec=False):
    if use_codec:
        with codecs.open(filename, encoding='utf-8', mode='w') as fp:
            fp.write(unicode(content))
    else:
        with open(filename, mode='w') as fp:
            fp.write(content)


def append_to_file(filename, content):
    with open(filename, mode='a+') as fp:
        fp.write(content)


def get(arr, indx):
    try:
        return arr[indx]
    except IndexError:
        return None


def remove_extra_whitespace(txt):
    return re.sub(' +', ' ', txt)


def cleanup_text(text):
    t = text.strip()
    t = re.sub('\t+', '', t)
    t = re.sub('\n+', '\n', t)
    t = re.sub(' +', ' ', t)
    t = re.sub('\xa0', '', t)
    t = re.sub('\u2022', '', t)
    return t


def clean_url(lnk, baseurl):
    lnk = lnk.replace('.html', '')
    lnk = lnk.replace('.htm', '')
    lnk = lnk.replace(baseurl, '')
    return lnk.lower()


def union(l1, l2):
    a = l1.copy()
    b = l2.copy()
    for e in a[:]:
        if e in b:
            a.remove(e)
            b.remove(e)
    return a, b


def joindict(d1, d2):
    d = d1.copy()
    d.update(d2)
    return d


def hash(url, data=None):
    """ creates hash of the url and post data (if required and exists)"""
    m = md5()
    m.update(url)
    if data is not None:
        m.update(data)
    return m.hexdigest()


def setup_logger():
    logger = logging.getLogger(config.g('logger.base'))
    level = getattr(logging, config.g('logger.level'))
    clevel = getattr(logging, config.g('logger.console.level'))
    flevel = getattr(logging, config.g('logger.file.level'))
    logger.setLevel(level)
    fh = logging.FileHandler(config.g('logger.path'))
    ch = logging.StreamHandler()
    template = config.get('logger', 'template')
    fm = logging.Formatter(template)
    fm.datefmt = config.g('logger.datefmt')
    fh.setFormatter(fm)
    ch.setFormatter(fm)
    ch.setLevel(clevel)
    fh.setLevel(flevel)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def dict_g(d, ky, default=False):
    keys = ky.split('.')
    k = d
    for kw in keys:
        if kw not in k:
            return default
        k = k[kw]
    return k


def dict_s(d, ky, val):
    keys = ky.split('.')
    k = d
    if len(keys) == 1:
        d[ky] = val
    while True:
        kw = keys.pop(0)
        if kw not in k:
            if len(keys) == 0:
                k[kw] = None
            else:
                k[kw] = {}
        if type(k[kw]).isisntance(type({})):
            k[kw] = val
            break
        else:
            k = k[kw]


def flat_rows(listing):
    rows = []
    for item in listing:
        rows.append(item[0])
    return '\n'.join(rows)


class DateTimeEncoder(json.JSONEncoder):
    """ encode datetime to proper string for json
        DateTimeEncoder().encode(object)
    """
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return (datetime.datetime.min + obj).time().isoformat()
        else:
            return super(DateTimeEncoder, self).default(obj)


