from datetime import datetime
import codecs
import logging
import logging.handlers
import os
import re
import json
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
try:
    import libs.config as config
except ImportError:
    import config
try:
    from hashlib import md5
except ImportError:
    from md5 import md5


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
        raise e


def uni(text):
    """get some unicode love

    :text: @todo
    :returns: @todo

    """
    try:
        return unicode(text)
    except NameError:
        return text


def save_to_file(filename, content, use_codec=False):
    if use_codec:
        with codecs.open(filename, encoding='utf-8', mode='w') as fp:
            try:
                return fp.write(unicode(content))
            except NameError:
                pass
            try:
                return fp.write(content.encode('utf-8'))
            except TypeError:
                pass
            return fp.write(content)
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
    return content


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


def setup_logger(load_cfg=None):
    """sets up logging"""
    # {{{ load config
    if load_cfg != None:
        cfg = config.Config(load_cfg)
    else:
        cfg = config.Config()
    # }}}
    # {{{ setting up everything
    logger = logging.getLogger(cfg.g('logger.base'))
    level = getattr(logging, cfg.g('logger.level'))
    clevel = getattr(logging, cfg.g('logger.console.level'))
    flevel = getattr(logging, cfg.g('logger.file.level'))
    logger.setLevel(level)
    logfilepath = cfg.g('logger.path')
    maxsize = cfg.g('logger.backupsize', default=33554432)
    filehandler = logging.handlers.RotatingFileHandler(logfilepath,
                                                       mode='w',
                                                       maxBytes=maxsize,
                                                       backupCount=2)
    template = cfg.get('logger', 'template')
    formatter = logging.Formatter(template)
    formatter.datefmt = cfg.g('logger.datefmt')
    # }}}
    # {{{ configure handlers
    console_off = cfg.g('logger.console.off', 'no')
    if console_off == 'no':
        consolehandler = logging.StreamHandler()
        consolehandler.setFormatter(formatter)
        consolehandler.setLevel(clevel)
        logger.addHandler(consolehandler)
    filehandler.setFormatter(formatter)
    filehandler.setLevel(flevel)
    logger.addHandler(filehandler)
    # }}}
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


def is_valid_cache_file(fullpath, notlessthan=100):
    """check if given file path is not an incomplete
    html file"""
    if not os.path.exists(fullpath):
        return False
    statinfo = os.stat(fullpath)
    return statinfo.st_size > notlessthan


def get_cache_full_path(url, post=None):
    """generate full path for given URL with POST data

    :url:
    :post: post data to pass
    :returns: full path of the cache file

    """
    filename = hash(url, post)
    return file_cached_path(filename, url)


def clean_failed_page_cache(url, post=None):
    """
    remove cached files that failed
    """
    fullpath = get_cache_full_path(url, post)
    if os.path.exists(fullpath):
        os.unlink(fullpath)


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


def get_net_loc(url):
    """get net location without domain

    :url: url to clean
    :returns: @todo

    """
    urlobj = urlparse(url.replace('www.', ''))
    netloc = urlobj.netloc.split('.')
    if len(netloc) > 2:
        return '.'.join(netloc[1:])
    else:
        return urlobj.netloc


def get_shorted_url(url, length=10):
    """cuts url for logger """
    try:
        segs = url.netloc.split('www.')[1][:length]
        return (''.join(segs)).center(length + 4, '_')
    except IndexError:
        return (''.join(url.netloc[:length])).center(length + 4, '_')
    except AttributeError:
        urlobj = urlparse(url)
    try:
        segs = urlobj.netloc.split('www.')[1][:length]
        return (''.join(segs)).center(length + 4, '_')
    except IndexError:
        return (''.join(urlobj.netloc[:length])).center(length + 4, '_')


def get_domain(url, tlds):
    """extracts top level domain"""
    try:
        url_elements = urlparse(url)[1].split('.')
    except TypeError:
        return ValueError("Failed to check url")
    for i in range(-len(url_elements), 0):
        last_i_elements = url_elements[i:]
        #    i=-3: ["abcde","co","uk"]
        #    i=-2: ["co","uk"]
        #    i=-1: ["uk"] etc
        # abcde.co.uk, co.uk, uk
        candidate = ".".join(last_i_elements)
        # *.co.uk, *.uk, *
        wildcard_candidate = ".".join(["*"] + last_i_elements[1:])
        exception_candidate = "!" + candidate
        # match tlds:
        if (exception_candidate in tlds):
            return ".".join(url_elements[i:])
        if (candidate in tlds or wildcard_candidate in tlds):
            return ".".join(url_elements[i - 1:])
            # returns "abcde.co.uk"
    raise ValueError("Domain not in global list of TLDs")


def older_than(filepath, hours):
    """check if file path is older than given time

    :filepath: file to check
    :hours: how many hours
    :returns: true/false
    """
    mtime = os.path.getmtime(filepath)
    import time
    ctime = time.time()
    time_passed = (ctime - mtime) / 3600
    if time_passed > hours:
        return True
    return False


def get_tlds():
    """load tld"""
    from libs.pages import DownloadedPage
    from libs.downloader import BaseDownloader
    fname = "effective_tld_names.dat.txt"
    if not os.path.exists(fname) or older_than(fname, 12):
        url = 'https://publicsuffix.org/list/effective_tld_names.dat'
        dlm = BaseDownloader()
        page = DownloadedPage().set_url(url)
        dlm.download(page=page)
        save_to_file(fname, page.text)
    with open("effective_tld_names.dat.txt") as tld_file:
        tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
    return tlds
