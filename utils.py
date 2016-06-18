from datetime import datetime
import codecs
import logging
import logging.handlers
import os
try:
    import libs.config as config
except ImportError:
    import config
import re
import json

try:
    from hashlib import md5
except ImportError as e:
    from md5 import md5

config = config.Config()


def read_file(filename, linewise=False):
    """
    reads a file, either as string or by line

    """
    try:
        with open(filename) as fptr:
            content = fptr.read().strip()
            if linewise:
                content = content.split("\n")
        return content
    except Exception as e:
        print(filename)
        raise e


def save_to_file(filename, content, use_codec=False):
    if use_codec:
        with codecs.open(filename, encoding='utf-8', mode='w') as fp:
            try:
                fp.write(unicode(content))
            except NameError:
                fp.write(content.encode('utf-8'))
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


def remove_br(content):
    """removes <br> tag

    :content: @todo
    :returns: @todo

    """
    content = content.replace('<br>', '\n')
    content = content.replace('</br>', '\n')
    content = content.replace('<br />', '\n')
    content = content.replace('<br%20/>', '\n')


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
    m.update(url.encode('utf-8'))
    if data is not None:
        m.update(data)
    return m.hexdigest()


def setup_logger():
    logger = logging.getLogger(config.g('logger.base'))
    level = getattr(logging, config.g('logger.level'))
    clevel = getattr(logging, config.g('logger.console.level'))
    flevel = getattr(logging, config.g('logger.file.level'))
    logger.setLevel(level)
    logfilepath = config.g('logger.path')
    maxsize = config.g('logger.backupsize', default=33554432)
    filehandler = logging.handlers.RotatingFileHandler(logfilepath,
                                              mode='w',
                                              maxBytes=maxsize,
                                              backupCount=2)
    consolehandler = logging.StreamHandler()
    template = config.get('logger', 'template')
    formatter = logging.Formatter(template)
    formatter.datefmt = config.g('logger.datefmt')
    filehandler.setFormatter(formatter)
    consolehandler.setFormatter(formatter)
    consolehandler.setLevel(clevel)
    filehandler.setLevel(flevel)
    logger.addHandler(filehandler)
    logger.addHandler(consolehandler)
    return logger


def dict_g(dct, key, default=False):
    keys = key.split('.')
    k = dct
    for kwrd in keys:
        if kwrd not in k:
            return default
        k = k[kwrd]
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


def search_line_in_file(filename, text):
    """searchs a text linewise in a file

    :filename: @todo
    :text: @todo
    :returns: @todo

    """
    with open(filename) as f:
        return text in f
    return False

def get_timestamp():
    """get current unix timestamp
    :returns: @todo

    """
    return (datetime.now() - datetime(1970, 1, 1)).total_seconds()


def file_cached_path(filename, url=None):
    """ expects hashed filename """
    burl = ''
    if url:
        burl = url.replace('http://', '')
        burl = burl.replace('https://', '')
        burl = burl.replace('www.', '')
        burl = burl.split('/')[0]
    segsize = 3
    cachepath = 'cache'
    firstpart = filename[0:segsize]
    secondpart = filename[segsize: 2 * segsize]
    fullpath = "%s/%s/%s/%s" % (cachepath, burl, firstpart, secondpart)
    if not os.path.exists(fullpath):
        os.makedirs(fullpath)
    return '%s/%s.html' % (fullpath, filename)

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


def delete_folder_content(folder, delete_parent=False):
    """delete a folders content

    :folder: @todo
    :returns: @todo

    """
    import os
    import shutil
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)
    if delete_parent:
        os.rmdir(folder)
