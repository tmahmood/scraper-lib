import libs.config as config
import copy
import logging
from lxml.etree import XMLSyntaxError
from lxml import html
from lxml.html.clean import Cleaner
import os
import requests
import time
import libs.utils as utils

USER_AGENT = 'Mozilla/5.0 Gecko/20120101 Firefox/20.0'
SLEEP_AFTER = 10
SLEEP = 3
g_config = config.Config()
logger = None


class BaseDownloader(object):
    """docstring for BaseDownloader"""
    def __init__(self):
        super(BaseDownloader, self).__init__()
        global logger
        logger = logging.getLogger('{}.dm'.format(g_config.g('logger.base')))
        self.debug = 0
        self.downloads = 0
        self.content = ''
        self.content_non_unicode = ''
        self.status_code = 'N/A'
        self.opener = None
        self.testing = False
        self.headers = {'USER_AGENT': USER_AGENT}

    def check_return(self, err):
        if self.testing:
            return False
        else:
            raise err

    def download(self, url, post=None):
        error_count = 0
        while True:
            try:
                with requests.Session() as s:
                    s.headers.update(self.headers)
                    if post != None:
                        response = s.post(url, post)
                    else:
                        response = s.get(url)
                    self.status_code = response.status_code
                    self.url = response.url
                    content = response.text
                    content_non_unicode = response.content
                break
            except requests.ConnectionError as err:
                url = url.replace(' ', '%20').lower()
                url = url.replace('<br%20>', '')
                url = url.replace('<br%20/>', '')
                if 'code' in err:
                    self.status_code = err.code
                    logger.info('received code: %s', error)
                    if err.code == 404:
                        error_count = 6
                error_count = error_count + 1
                if error_count > 3 and error_count < 5:
                    logger.error('error! will retry %s', url)
                    time.sleep(10)
                    continue
                if error_count > 5:
                    logger.exception('Too many failuers, I giveup %s', url)
                    return self.check_return(err)
            except Exception as err:
                logger.exception('failed to download: %s', url)
                return self.check_return(err)
        self.content = content
        self.content_non_unicode = content_non_unicode
        self.last_url = response.url
        self.downloads = self.downloads + 1
        self.take_a_nap_after(SLEEP_AFTER, SLEEP)
        return True

    def take_a_nap_after(self, after, duration):
        if self.downloads % after == 0:
            time.sleep(duration)


class DomDownloader(BaseDownloader):

    def __init__(self):
        super(DomDownloader, self).__init__()
        global logger
        logger = logging.getLogger('{}.dm.dom'.format(g_config.g('logger.base')))

    def remove_br(self, content):
        """removes <br> tag

        :content: @todo
        :returns: @todo

        """
        content = content.replace('<br>', '\n')
        content = content.replace('</br>', '\n')
        content = content.replace('<br />', '\n')
        content = content.replace('<br%20/>', '\n')

    def download(self, url=None, post=None, remove_br=False):
        state = super(DomDownloader, self).download(url, post)
        if not state:
            logger.error('download failed')
            return None
        content = self.content
        while True:
            try:
                if remove_br:
                    self.remove_br(content)
                self.dom = html.fromstring(content)
                break
            except ValueError:
                content = self.content_non_unicode
            except XMLSyntaxError:
                logger.exception('failed parsing content')
                break

    def make_links_absolute(self, link):
        self.dom_orig = copy.deepcopy(self.dom)
        self.dom.make_links_absolute(link)

    def clean_dom(self):
        cleaner = Cleaner()
        cleaner.script = True
        cleaner.style = True
        cleaner.comments = True
        return cleaner.clean_html(self.dom)


class CachedDownloader(BaseDownloader):

    def __init__(self):
        super(CachedDownloader, self).__init__()
        global logger
        logger = logging.getLogger('{}.dm.cached'.format(g_config.g('logger.base')))

    def download(self, url, post=None):
        self.content = ''
        self.url = url
        filename = utils.hash(url, post)
        fullpath = self.file_cached_path(filename, url)
        if os.path.exists(fullpath):
            self.content = utils.read_file(fullpath)
            self.from_cache = True
            return True
        self.from_cache = False
        state = super(CachedDownloader, self).download(url, post)
        if state:
            utils.save_to_file(fullpath, self.content, True)
            return True
        return False

    def clean_failed_page_cache(self, url, post=None):
        filename = utils.hash(url, post)
        fullpath = self.file_cached_path(filename)
        if os.path.exists(fullpath):
            os.unlink(fullpath)

    def file_cached_path(self, filename, url=None):
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


class CachedDomLoader(DomDownloader, CachedDownloader):
    def __init__(self):
        super(CachedDomLoader, self).__init__()
        global logger
        logger = logging.getLogger('{}.dm.cached_dom'.format(g_config.g('logger.base')))
