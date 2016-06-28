"""
downloader
"""
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
import unittest
import random
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

# pylint: disable=global-statement

USER_AGENT = 'Mozilla/5.0 Gecko/20120101 Firefox/20.0'
SLEEP_AFTER = 10
SLEEP = 3


def cleanup_url(url):
    """cleans up the given url of weird stuffs

    :url: @todo
    :returns: @todo

    """
    url = url.replace(' ', '%20').lower()
    url = url.replace('<br%20>', '')
    url = url.replace('<br%20/>', '')
    return url


# pylint: disable=too-few-public-methods
class Logger(object):
    """logger class"""

    cfg = None
    log = None

    def __init__(self):
        super(Logger, self).__init__()
        Logger.cfg = config.Config()
        txt = '{}.dm'.format(Logger.cfg.g('logger.base'))
        Logger.log = logging.getLogger(txt)


# pylint: disable=too-many-instance-attributes
class BaseDownloader(Logger):
    """docstring for BaseDownloader"""

    def __init__(self):
        super(BaseDownloader, self).__init__()
        self.url = None
        self.debug = 0
        self.downloads = 0
        self.content = ''
        self.last_url = None
        self.content_non_unicode = ''
        self.status_code = 'N/A'
        self.timeout = Logger.cfg.g('timeout', 60)
        self.opener = None
        self.from_cache = False
        self.testing = False
        self.current_proxy = None
        self.proxy_used = 0
        self.bad_proxies = set()
        self.headers = {'USER_AGENT': USER_AGENT}
        self.load_bad_proxies()

    def proxy_enabled(self):
        """check if proxy is enabled
        :returns: @todo

        """
        return Logger.cfg.g('proxies', 'no') == 'yes'

    def load_bad_proxies(self):
        """loads up bad proxies
        :returns: @todo

        """
        if self.proxy_enabled():
            self.current_proxy = self.get_random_proxy()
            try:
                self.bad_proxies = set(utils.read_file('bad_proxies', True))
            except OSError:
                pass

    def get_random_proxy(self):
        """returns a proxy from proxies.txt

        :returns: @todo
        """
        proxy_file = Logger.cfg.g('proxy_file', 'proxies.txt')
        proxies = utils.read_file(proxy_file, True)
        while True:
            proxy = random.choice(proxies)
            if proxy in self.bad_proxies:
                continue
            self.proxy_used = 0
            return proxy

    def check_return(self, err):
        """
        what are we returning
        """
        if self.testing:
            return False
        else:
            raise err

    def download(self, url, post=None):
        """
        downloads given link
        """
        if self.proxy_used >= 100:
            old_proxy = self.current_proxy
            self.current_proxy = self.get_random_proxy()
            Logger.log.info("proxy: %s -> %s", old_proxy, self.current_proxy)
        proxy = {'http': self.current_proxy}
        url = cleanup_url(url)
        try:
            response = self._download(url, post, proxy)
        except requests.ConnectionError:
            return False
        self.url = response.url
        content = response.text
        content_non_unicode = response.content
        self.status_code = response.status_code
        self.content = content
        self.content_non_unicode = content_non_unicode
        self.last_url = response.url
        self.downloads = self.downloads + 1
        self.take_a_nap_after(SLEEP_AFTER, SLEEP)
        return True

    def _download(self, url, post=None, proxy=None):
        """does the actual download

        :url: @todo
        :post: @todo
        :returns: @todo

        """
        error_count = 0
        while True:
            try:
                with requests.Session() as session:
                    session.headers.update(self.headers)
                    if post != None:
                        response = session.post(url, post, proxies=proxy,
                                                timeout=self.timeout)
                    else:
                        response = session.get(url, proxies=proxy,
                                               timeout=self.timeout)
                if self.proxy_enabled():
                    self.proxy_used += 1
                return response
            except requests.exceptions.Timeout:
                Logger.log.error("Timed out: %s", url)
                raise requests.ConnectionError()
            except requests.packages.urllib3.exceptions.ReadTimeoutError:
                Logger.log.exception("%s", url)
                raise requests.ConnectionError()
            except requests.exceptions.SSLError:
                Logger.log.exception("%s", url)
                raise requests.ConnectionError()
            except requests.exceptions.ProxyError:
                self.bad_proxies.add(proxy['http'])
                utils.append_to_file('bad_proxies', proxy['http'] + '\n')
                self.current_proxy = self.get_random_proxy()
                proxy = {'http': self.current_proxy}
                error_count += 1
                if error_count < 3:
                    continue
                self.current_proxy = self.get_random_proxy()
                raise requests.ConnectionError()
            except requests.ConnectionError:
                Logger.log.exception('Failed to parse: ', url)
                raise requests.ConnectionError()

    def take_a_nap_after(self, after, duration):
        """force sleep :after: for :duration:"""
        if self.downloads % after == 0:
            time.sleep(duration)


class DomDownloader(BaseDownloader):
    """sets up DOM to parse html document"""
    def __init__(self):
        super(DomDownloader, self).__init__()
        self.dom = None
        self.dom_orig = None
        self.remove_br = False

    def download(self, url=None, post=None):
        """downloads by URL and set DOM for the URL"""
        state = super(DomDownloader, self).download(url, post)
        if not state:
            Logger.log.error('download failed')
            return None
        content = self.content
        tried_non_unicode = False
        tried_soup = False
        while True:
            try:
                if self.remove_br:
                    content = utils.remove_br(content)
                dom = html.fromstring(content)
                self.dom = Dom(dom, 0)
                break
            except ValueError:
                if tried_non_unicode is True:
                    Logger.log.exception("failed to unicode")
                    break
                tried_non_unicode = True
                content = self.content_non_unicode
            except XMLSyntaxError:
                if tried_soup:
                    Logger.log.exception("soup spoiled")
                    break
                Logger.log.error('lxml failed, trying soup')
                from lxml.html.soupparser import fromstring
                dom = fromstring(content)
                self.dom = Dom(dom, 1)
                tried_soup = True
                break

    def clean_dom(self):
        """get rids of script, style and comments"""
        cleaner = Cleaner()
        cleaner.script = True
        cleaner.style = True
        cleaner.comments = True
        return cleaner.clean_html(self.dom)


class CachedDownloader(BaseDownloader):
    """downloads and save webpages"""

    def __init__(self):
        super(CachedDownloader, self).__init__()

    def download(self, url, post=None):
        self.content = ''
        self.url = url
        filename = utils.hash(url, post)
        fullpath = utils.file_cached_path(filename, url)
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
        """
        remove cached files that failed
        """
        filename = utils.hash(url, post)
        fullpath = utils.file_cached_path(filename)
        if os.path.exists(fullpath):
            os.unlink(fullpath)


class CachedDomLoader(DomDownloader, CachedDownloader):
    """
    use cache and dom
    """
    def __init__(self):
        super(CachedDomLoader, self).__init__()


class Dom(object):
    """dom helper,

    incase we have to switch to beautifulsoup parser
    """

    def __init__(self, dom, dom_type=0):
        super(Dom, self).__init__()
        self.dom = dom
        self.dom_type = dom_type

    def first(self, xpath):
        """gets the first element from the result"""
        elist = self.xpath(xpath)
        try:
            return elist[0]
        except IndexError:
            return None

    def attr(self, xpath, attr):
        """get [attr] of element at [index] from the result"""
        elm = self.first(xpath)
        try:
            return elm[0].attrib[attr]
        except IndexError:
            return None

    def text(self, xpath, index=0):
        """get text of element at [index] from the result"""
        elist = self.xpath(xpath)
        try:
            return elist[index].text_content()
        except IndexError:
            return None

    def xpath(self, xpath):
        """use xpath

        :xpath: @todo
        :returns: @todo

        """
        if self.dom_type == 1:
            return self.dom.find(xpath)
        return self.dom.xpath(xpath)

    def make_links_absolute(self, link):
        """calls make_links_absolute
        :returns: @todo

        """
        self.dom.make_links_absolute(link)


class TestDownloader(unittest.TestCase):
    """docstring for TestDownloader"""

    codes = {
        '200': 'OK',
        '201': 'Created',
        '202': 'Accepted',
        '203': 'Non-Authoritative Information',
        '204': 'No Content',
        '205': 'Reset Content',
        '206': 'Partial Content',
        '300': 'Multiple Choices',
        '301': 'Moved Permanently',
        '302': 'Found',
        '303': 'See Other',
        '304': 'Not Modified',
        '305': 'Use Proxy',
        '306': 'Unused',
        '307': 'Temporary Redirect',
        '308': 'Permanent Redirect',
        '400': 'Bad Request',
        '401': 'Unauthorized',
        '402': 'Payment Required',
        '403': 'Forbidden',
        '404': 'Not Found',
        '405': 'Method Not Allowed',
        '406': 'Not Acceptable',
        '407': 'Proxy Authentication Required',
        '408': 'Request Timeout',
        '409': 'Conflict',
        '410': 'Gone',
        '411': 'Length Required',
        '412': 'Precondition Required',
        '413': 'Request Entry Too Large',
        '414': 'Request-URI Too Long',
        '415': 'Unsupported Media Type',
        '416': 'Requested Range Not Satisfiable',
        '417': 'Expectation Failed',
        '418': "I'm a teapot",
        '422': 'Unprocessable Entity',
        '428': 'Precondition Required',
        '429': 'Too Many Requests',
        '431': 'Request Header Fields Too Large',
        '451': 'Unavailable For Legal Reasons',
        '500': 'Internal Server Error',
        '501': 'Not Implemented',
        '502': 'Bad Gateway',
        '503': 'Service Unavailable',
        '504': 'Gateway Timeout',
        '505': 'HTTP Version Not Supported',
        '511': 'Network Authentication Required',
        '520': 'Web server is returning an unknown error',
        '522': 'Connection timed out',
        '524': 'A timeout occurred',
    }

    def test_base_downloader(self):
        """tests base downloader"""
        pass


def main():
    """do some tests"""
    unittest.main()


if __name__ == '__main__':
    main()
