import logging
import urllib2
from urllib2 import HTTPRedirectHandler, HTTPHandler, HTTPCookieProcessor
import cookielib
import os
import time
from lxml import html
from lxml.html.clean import Cleaner
import utils
import config
from lxml.etree import XMLSyntaxError
import copy

USER_AGENT = 'Mozilla/5.0 Gecko/20120101 Firefox/20.0'
g_config = config.Config()


class BaseDownloader(object):
    """docstring for BaseDownloader"""
    def __init__(self):
        super(BaseDownloader, self).__init__()
        l = '{}.dm'.format(g_config.g('logger.base'))
        self.logger = logging.getLogger(l)
        self.debug = 0
        self.downloads = 0
        self.content = ''
        self.opener = None
        self.init_opener()

    def init_opener(self):
        if self.opener is not None:
            self.opener.close()
            self.downloads = 0
        self.cj = cookielib.LWPCookieJar('cookie.jar')
        self.opener = urllib2.build_opener(HTTPRedirectHandler(),
                                           HTTPHandler(debuglevel=self.debug),
                                           HTTPCookieProcessor(self.cj))
        self.opener.addheaders = [('User-agent', USER_AGENT)]

    def download(self, url, post=None):
        error_count = 0
        while True:
            try:
                response = self.opener.open(url.strip('?'), post, timeout=30)
                rtxt = ''.join(response.readlines())
                self.url = response.url
                content = unicode(rtxt, errors='ignore')
                break
            except (urllib2.URLError, urllib2.HTTPError) as err:
                url = url.replace(' ', '%20')
                if 'code' in err and err.code == 404:
                    self.logger.info('404 error')
                    error_count = 6
                error_count = error_count + 1
                if error_count > 3 and error_count < 5:
                    self.logger.error('error! new opener %s', url)
                    time.sleep(10)
                    self.init_opener()
                    continue
                if error_count > 5:
                    self.logger.exception('Too many failuers, I giveup %s', url)
                    raise err
            except Exception as err:
                self.logger.exception('failed to download: %s', url)
                raise err
        self.content = content
        self.last_url = response.url
        self.downloads = self.downloads + 1
        self.take_a_nap_after(10, 5)
        return True

    def take_a_nap_after(self, after, duration):
        if self.downloads % after == 0:
            time.sleep(duration)


class DomDownloader(BaseDownloader):

    def __init__(self):
        super(DomDownloader, self).__init__()
        l = '{}.dm.dom'.format(g_config.g('logger.base'))
        self.logger = logging.getLogger(l)

    def download(self, url=None, post=None, remove_br=False):
        state = super(DomDownloader, self).download(url, post)
        if not state:
            self.logger.error('download failed')
            return None
        content = self.content
        if remove_br:
            content = content.replace('<br>', '\n')
            content = content.replace('</br>', '\n')
            content = content.replace('<br />', '\n')
        try:
            self.dom = html.fromstring(content)
        except XMLSyntaxError:
            self.logger.exception('failed')
            pass

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
        l = '{}.dm.cached'.format(g_config.g('logger.base'))
        self.logger = logging.getLogger(l)

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
        l = '{}.dm.cached_dom'.format(g_config.g('logger.base'))
        self.logger = logging.getLogger(l)
