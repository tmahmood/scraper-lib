import logging
import urllib2
import httplib
import cookielib
import random
import socket
import os
import time
from lxml import html, etree
from lxml.html.clean import clean_html, Cleaner
import utils
import config

USER_AGENT = 'Mozilla/5.0 Gecko/20120101 Firefox/20.0'
g_config = config.Config()

class BaseDownloader(object):
    """docstring for BaseDownloader"""
    def __init__(self):
        super(BaseDownloader, self).__init__()
        l = '{}.dm'.format(g_config.g('logger.base'))
        self.logger = logging.getLogger(l)
        self.verbose = 0
        self.downloads = 0
        self.init_opener()

    def init_opener(self):
        if hasattr(self, 'opener'):
            self.opener.close()
            self.downloads = 0
        self.cj = cookielib.LWPCookieJar('cookie.jar')
        self.opener = urllib2.build_opener(
                urllib2.HTTPRedirectHandler(),
                urllib2.HTTPHandler(debuglevel=self.verbose),
                urllib2.HTTPCookieProcessor(self.cj),
                )
        self.opener.addheaders = [('User-agent', USER_AGENT)]

    def download(self, url, post=None):
        """ Downloads url, if post is not None then
        makes a post request

        :url: Url to send request to
        :post: data to be post
        :returns: True or False depending on result
        """
        self.content = ''
        error_count = 0
        while True:
            try:
                response = self.opener.open(url.strip('?'), post, timeout=30)
                content = ''.join(response.readlines())
                break
            except (urllib2.URLError, urllib2.HTTPError) as e:
                self.logger.error('error occurred {}'.format(url))
                error_count = error_count + 1
                if error_count > 3 and error_count < 5:
                    time.sleep(10)
                    self.init_opener()
                    continue
                if error_count > 5:
                    raise e
            except Exception as e:
                print ('error')
                raise e
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

    def download(self, url=None, post=None, remove_br = False):
        state = super(DomDownloader, self).download(url, post)
        if not state:
           self.logger.error('download failed')
           return None
        content = self.content
        if remove_br:
            content = content.replace('<br>', '\n')
            content = content.replace('</br>', '\n')
            content = content.replace('<br />', '\n')
        self.dom = html.fromstring(content)
        return True

    def clean_dom(self, baseurl):
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
        filename = utils.hash(url, post)
        fullpath = self.file_cached_path(filename, url)
        if os.path.exists(fullpath):
            self.content = utils.read_file(fullpath)
            self.from_cache = True
            return True
        self.from_cache = False
        error = 0
        state = super(CachedDownloader, self).download(url, post)
        if state:
            utils.save_to_file(fullpath, self.content)
            return True
        return False


    def clean_failed_page_cache(self, url, post = None):
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
        firstpart  = filename[0:segsize]
        secondpart = filename[segsize : 2 * segsize]
        fullpath = "%s/%s/%s/%s" % (cachepath, burl, firstpart, secondpart)
        if not os.path.exists(fullpath):
            os.makedirs(fullpath)
        return '%s/%s.html' % (fullpath, filename)



class CachedDomLoader(DomDownloader, CachedDownloader):
    def __init__(self):
        super(DomDownloader, self).__init__()
        l = '{}.dm.cached_dom'.format(g_config.g('logger.base'))
        self.logger = logging.getLogger(l)
