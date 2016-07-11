"""

downloader
"""
import logging
import requests
import time
import unittest
import random
try:
    import libs.config as config
    import libs.utils as utils
    import libs.pages as pages
except ImportError:
    import config
    import utils
    import pages

# pylint: disable=global-statement

USER_AGENT = 'Mozilla/5.0 Gecko/20120101 Firefox/40.0'
SLEEP_AFTER = 10
SLEEP = 3


class ResponseText(object):
    """store response text"""
    def __init__(self):
        super(ResponseText, self).__init__()
        self.text = None
        self.raw_text = None


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

    def __init__(self):
        super(Logger, self).__init__()
        self.cfg = config.Config()
        txt = '{}.dm'.format(self.cfg.g('logger.base'))
        self.log = logging.getLogger(txt)


class BaseDownloader(Logger):
    """docstring for BaseDownloader"""

    def __init__(self):
        super(BaseDownloader, self).__init__()
        self.downloads = 0
        self.timeout = self.cfg.g('timeout', 60)
        self.from_cache = False
        self.current_proxy = None
        self.proxy_used = 0
        self.bad_proxies = set()
        self.headers = {'USER_AGENT': USER_AGENT}
        self.load_bad_proxies()

    def proxy_enabled(self):
        """check if proxy is enabled
        :returns: @todo

        """
        return self.cfg.g('proxies', 'no') == 'yes'

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
        proxy_file = self.cfg.g('proxy_file', 'proxies.txt')
        proxies = utils.read_file(proxy_file, True)
        while True:
            proxy = random.choice(proxies)
            if proxy in self.bad_proxies:
                continue
            self.proxy_used = 0
            return proxy

    def _download(self, url, post=None, proxy=None):
        """does the actual download"""
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
                self.log.error("Timed out: %s", url)
                raise requests.ConnectionError()
            except requests.packages.urllib3.exceptions.ReadTimeoutError:
                self.log.exception("%s", url)
                raise requests.ConnectionError()
            except requests.exceptions.SSLError:
                self.log.exception("%s", url)
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
                self.log.exception('Failed to parse: %s', url)
                raise requests.ConnectionError()

    def take_a_nap_after(self, after, duration):
        """force sleep :after: for :duration:"""
        if self.downloads % after == 0:
            time.sleep(duration)

    def download(self, page):
        """downloads given url"""
        if self.proxy_used >= self.cfg.g('proxy.used', 100):
            old_proxy = self.current_proxy
            self.current_proxy = self.get_random_proxy()
            self.log.info("proxy: %s -> %s", old_proxy, self.current_proxy)
        proxy = {'http': self.current_proxy}
        url = cleanup_url(page.url)
        if page.url != url:
            page.set_url(url)
        try:
            start_time = time.time()
            response = self._download(page.url, page.post, proxy)
            end_time = time.time()
            page.set_load_time(end_time - start_time)
            self.take_a_nap_after(SLEEP_AFTER, SLEEP)
        except requests.ConnectionError:
            self.log.debug("ConnectionError: %s", url)
        self.downloads = self.downloads + 1
        try:
            page.set_text(response.text, response.content) \
                .set_status_code(response.status_code) \
                .set_last_url(response.url)
        except UnboundLocalError:
            page.set_state(False)


class CachedDownloader(BaseDownloader):
    """downloads and save webpages"""

    def download(self, page):
        content = ''
        fullpath = utils.get_cache_full_path(page.url, page.post)
        if utils.is_valid_cache_file(fullpath):
            content = utils.read_file(fullpath)
            page.set_text(content).set_last_url(page.url).set_load_time(0)
        else:
            super(CachedDownloader, self).download(page)
            if page.state:
                utils.save_to_file(fullpath, page.text, True)


CODES = {
    '200': [True, 'OK'],
    '201': [True, 'Created'],
    '202': [True, 'Accepted'],
    '203': [True, 'Non-Authoritative Information'],
    '204': [True, 'No Content'],
    '205': [True, 'Reset Content'],
    '206': [True, 'Partial Content'],
    '300': [True, 'Multiple Choices'],
    '301': [True, 'Moved Permanently'],
    '302': [True, 'Found'],
    '303': [True, 'See Other'],
    '304': [True, 'Not Modified'],
    '305': [True, 'Use Proxy'],
    '306': [True, 'Unused'],
    '307': [True, 'Temporary Redirect'],
    '308': [True, 'Permanent Redirect'],
    '400': [False, 'Bad Request'],
    '401': [False, 'Unauthorized'],
    '402': [False, 'Payment Required'],
    '403': [False, 'Forbidden'],
    '404': [False, 'Not Found'],
    '405': [False, 'Method Not Allowed'],
    '406': [False, 'Not Acceptable'],
    '407': [False, 'Proxy Authentication Required'],
    '408': [False, 'Request Timeout'],
    '409': [False, 'Conflict'],
    '410': [False, 'Gone'],
    '411': [False, 'Length Required'],
    '412': [False, 'Precondition Required'],
    '413': [False, 'Request Entry Too Large'],
    '414': [False, 'Request-URI Too Long'],
    '415': [False, 'Unsupported Media Type'],
    '416': [False, 'Requested Range Not Satisfiable'],
    '417': [False, 'Expectation Failed'],
    '418': [False, "I'm a teapot"],
    '422': [False, 'Unprocessable Entity'],
    '428': [False, 'Precondition Required'],
    '429': [False, 'Too Many Requests'],
    '431': [False, 'Request Header Fields Too Large'],
    '451': [False, 'Unavailable For Legal Reasons'],
    '500': [False, 'Internal Server Error'],
    '501': [False, 'Not Implemented'],
    '502': [False, 'Bad Gateway'],
    '503': [False, 'Service Unavailable'],
    '504': [False, 'Gateway Timeout'],
    '505': [False, 'HTTP Version Not Supported'],
    '511': [False, 'Network Authentication Required'],
    '520': [False, 'Web server is returning an unknown error'],
    '522': [False, 'Connection timed out'],
    '524': [False, 'A timeout occurred'],
}


class TestDownloaderBasics(unittest.TestCase):
    """docstring for TestDownloaderBasics"""
    def setUp(self):
        """clear cache folder
        """
        try:
            utils.delete_folder_content('cache/example.com')
        except Exception:
            pass

    def test_200(self):
        """test 200 status code"""
        dlm = BaseDownloader()
        page = pages.DownloadedPage().set_url('http://httpstat.us/200')
        dlm.download(page)
        self.assertTrue(page.state)
        self.assertEqual(page.status_code, 200)

    def test_301(self):
        """redirection
        :returns: @todo

        """
        dlm = BaseDownloader()
        page = pages.DownloadedPage().set_url('http://httpstat.us/301')
        dlm.download(page)
        self.assertTrue(page.state)
        self.assertEqual(200, page.status_code)
        self.assertEqual(page.last_url, 'http://httpstat.us')

    def test_404(self):
        """handling errors"""
        dlm = BaseDownloader()
        page = pages.DownloadedPage().set_url('http://httpstat.us/404')
        dlm.download(page)
        self.assertFalse(page.state)
        self.assertEqual(404, page.status_code)

    def test_404_web(self):
        """handling errors"""
        dlm = BaseDownloader()
        page = pages.DownloadedPage().set_url('http://192.155.84.35/scraper/sd')
        dlm.download(page)
        self.assertFalse(page.state)
        self.assertEqual(404, page.status_code)

    def test_timeout_fail(self):
        """handling errors"""
        dlm = BaseDownloader()
        dlm.timeout = 1
        page = pages.DownloadedPage().set_url('http://httpstat.us/524')
        dlm.download(page)
        self.assertFalse(page.state)
        self.assertEqual(524, page.status_code)

    def test_all_codes(self):
        """test with all possible status codes"""
        dlm = BaseDownloader()
        for code in CODES:
            info = CODES[code]
            url = 'http://httpstat.us/%s' % code
            page = pages.DownloadedPage().set_url(url)
            dlm.download(page)
            self.assertEqual(info[0], page.state)
            if int(code) >= 400:
                self.assertEqual(int(code), page.status_code)

    def test_dom(self):
        """test dom parsing and querying
        :returns: @todo

        """
        dlm = BaseDownloader()
        page = pages.DownloadedPage().set_url('http://example.com')
        dlm.download(page)
        dom = page.get_dom()
        result = dom.xpath('//h1')
        self.assertEqual(1, len(result))
        self.assertEqual('Example Domain', result[0].text_content().strip())
        self.assertEqual('More information...', dom.text('//a'))
        self.assertEqual('Example Domain', dom.first('//h1').text_content())
        self.assertEqual('More information...', dom.text('//p', 1))
        self.assertEqual("http://www.iana.org/domains/example",
                         dom.attr('//a', 'href'))

    def test_cached_page(self):
        """test run cached page class"""
        dlm = CachedDownloader()
        page = pages.DownloadedPage().set_url('http://example.com')
        dlm.download(page)
        dom = page.get_dom()
        result = dom.xpath('//h1')
        self.assertEqual(1, len(result))
        self.assertEqual('Example Domain', result[0].text_content().strip())
        self.assertEqual('More information...', dom.text('//a'))
        self.assertEqual('Example Domain', dom.first('//h1').text_content())
        self.assertEqual('More information...', dom.text('//p', 1))
        self.assertEqual("http://www.iana.org/domains/example",
                         dom.attr('//a', 'href'))

    def test_broken_html(self):
        """test on how to handle broken html files"""
        broken_html = """<meta/><head><title>Hello</head><body onload=crash()>
        Hi all<p><a href="google.com">google</a>"""
        page = pages.DownloadedPage().set_text(broken_html)
        dom = page.get_dom()
        self.assertEqual(dom.first('//title').text_content(), 'Hello')
        self.assertEqual(dom.attr('//a', 'href'), 'google.com')
        self.assertEqual(dom.text('//a'), 'google')


def main():
    logger = logging.getLogger('logger')
    logger.info('### start testing ###')
    unittest.main()


if __name__ == '__main__':
    main()
