"""

downloader
"""
import logging
import requests
import pycurl
from io import BytesIO
import time
import random
try:
    import libs.config as config
    import libs.utils as utils
except ImportError:
    # pylint: disable=relative-import
    import config
    import utils


USER_AGENT = 'Mozilla/5.0 Gecko/20120101 Firefox/40.0'
SLEEP_AFTER = 10
SLEEP = 3


class Error(Exception):
    """handles exceptions"""
    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RetryableError(Error):
    """docstring for ConnectionError"""
    def __init__(self, value=None):
        super(RetryableError, self).__init__()
        self.value = value


class SSLError(Error):
    """docstring for SSLError"""
    def __init__(self, value=None):
        super(SSLError, self).__init__()
        self.value = value


class ConnectionError(Error):
    """docstring for ConnectionError"""
    def __init__(self, value=None):
        super(ConnectionError, self).__init__()
        self.value = value


def request_factory(page, proxy, headers, timeout, logger=None):
    """uses request to download"""
    logging.getLogger("requests").setLevel(logging.WARNING)
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            if page.post != None:
                response = session.post(page.url, page.post, proxies=proxy,
                                        timeout=timeout)
            else:
                response = session.get(page.url, proxies=proxy, timeout=timeout)
        # download page and set response details
        page.set_text(response.text, response.content) \
            .set_status_code(response.status_code) \
            .set_redirected_to_url(response.url)
    except requests.exceptions.Timeout:
        logger.error("Timed out: %s", page.url)
        raise RetryableError('timed out')
    except requests.packages.urllib3.exceptions.ReadTimeoutError:
        logger.exception("%s", page.url)
        raise RetryableError('read timed out')
    except requests.exceptions.ProxyError:
        logger.exception("%s", page.url)
        raise RetryableError(proxy)
    except requests.exceptions.SSLError:
        logger.exception("%s", page.url)
        raise SSLError()
    except requests.exceptions.InvalidSchema:
        logger.exception('Failed to parse: %s', page.url)
        raise ConnectionError()
    except requests.ConnectionError:
        logger.exception('Failed to parse: %s', page.url)
        raise ConnectionError()


def curl_factory(page, proxy, headers, timeout, logger=None):
    """uses curl to download"""
    curl_headers = []
    for key in headers:
        curl_headers.append('%s: %s' % (key, headers[key]))
    curl_headers += ['Accept-Charset: UTF-8']
    response = BytesIO()
    headers = BytesIO()
    curl = pycurl.Curl()
    try:
        curl.setopt(curl.URL, page.url)
    except UnicodeEncodeError:
        logger.error("URL ISSUE: %s", page.url)
        raise Error()
    curl.setopt(curl.TIMEOUT, timeout)
    curl.setopt(curl.WRITEFUNCTION, response.write)
    curl.setopt(curl.HEADERFUNCTION, headers.write)
    curl.setopt(curl.HTTPHEADER, curl_headers)
    curl.setopt(curl.FOLLOWLOCATION, True)
    curl.setopt(curl.TIMEOUT, timeout * 2)
    if proxy != None:
        logger.debug("setting proxy: %s", proxy)
        curl.setopt(curl.PROXY, proxy['http'])
    if page.post is not None:
        logger.debug("setting post: %s", page.post)
        curl.setopt(curl.POSTFIELD, page.post)
    try:
        curl.perform()
    except pycurl.error:
        logger.exception('failed downloading')
        raise Error()
    text = response.getvalue().decode('UTF-8', errors='ignore')
    status_code = curl.getinfo(curl.RESPONSE_CODE)
    page.set_text(text, response).set_status_code(status_code)
    try:
        headers.seek(0)
        lines = headers.getvalue().decode('UTF-8').split('\r\n')
        redirected_to = page.url
        for line in lines:
            if 'Location' in line:
                redirected_to = line.split(': ')[-1]
        curl.close()
        page.set_redirected_to_url(redirected_to)
    except Exception:
        logger.exception('failed parsing headers')
        page.set_redirected_to_url(page.url)


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
class BaseCommon(object):
    """logger class"""

    def __init__(self):
        super(BaseCommon, self).__init__()
        self.cfg = config.Config()
        txt = '{}.dm'.format(self.cfg.g('logger.base'))
        self.log = logging.getLogger(txt)


class BaseDownloader(BaseCommon):
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
        self.use_proxy = self.cfg.g('proxies', 'no') == 'yes'
        self.use_curl = self.cfg.g('use_curl', 'no') == 'yes'
        self.load_bad_proxies()
        self.which_downloader()

    def set_logger(self, logger):
        """sets up independent logger

        :logger: @todo
        :returns: @todo

        """
        self.log = logger

    def which_downloader(self):
        """sets which downloader to be used"""
        if self.use_curl:
            self.download_with = curl_factory
        else:
            self.download_with = request_factory

    def proxy_enabled(self):
        """check if proxy is enabled
        :returns: @todo

        """
        return self.use_proxy

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

    def _download(self, page, proxy=None):
        """does the actual download"""
        error_count = 0
        while True:
            try:
                self.download_with(page, proxy, self.headers,
                                   self.timeout, self.log)
                if self.proxy_enabled():
                    self.proxy_used += 1
                return
            except ConnectionError:
                return
            except RetryableError:
                if self.proxy_enabled():
                    self.bad_proxies.add(proxy['http'])
                    utils.append_to_file('bad_proxies', proxy['http'] + '\n')
                    self.current_proxy = self.get_random_proxy()
                    proxy = {'http': self.current_proxy}
                error_count += 1
                if error_count > 3:
                    raise Error()

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
        if self.proxy_enabled():
            proxy = {'http': self.current_proxy}
        else:
            proxy = None
        url = cleanup_url(page.url)
        if page.url != url:
            page.set_url(url)
        try:
            start_time = time.time()
            self._download(page, proxy)
            end_time = time.time()
            page.set_load_time(end_time - start_time)
            self.take_a_nap_after(SLEEP_AFTER, SLEEP)
            self.downloads = self.downloads + 1
        except requests.ConnectionError:
            self.log.debug("ConnectionError: %s", url)
        except (UnboundLocalError, AttributeError, Error):
            page.set_state(False)


class CachedDownloader(BaseDownloader):
    """downloads and save webpages"""

    def download(self, page):
        content = ''
        fullpath = utils.get_cache_full_path(page.url, page.post)
        if utils.is_valid_cache_file(fullpath):
            content = utils.read_file(fullpath)
            page.set_text(content).set_redirected_to_url(page.url).set_load_time(0)
        else:
            super(CachedDownloader, self).download(page)
            if page.state:
                utils.save_to_file(fullpath, page.text, True)
