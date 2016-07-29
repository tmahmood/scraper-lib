import unittest
from downloader import BaseDownloader, CachedDownloader
from downloader import curl_factory
try:
    import libs.utils as utils
    import libs.pages as pages
except ImportError:
    import utils
    import pages


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
        dlm.download_with = USE_DOWNLOADER
        page = pages.DownloadedPage().set_url('http://httpstat.us/200')
        dlm.download(page)
        self.assertTrue(page.state)
        self.assertEqual(page.status_code, 200)

    def test_301(self):
        """redirection
        :returns: @todo

        """
        dlm = BaseDownloader()
        dlm.download_with = USE_DOWNLOADER
        page = pages.DownloadedPage().set_url('http://httpstat.us/301')
        dlm.download(page)
        self.assertTrue(page.state)
        self.assertEqual(200, page.status_code)
        self.assertEqual(page.last_url, 'http://httpstat.us')

    def test_404(self):
        """handling errors"""
        dlm = BaseDownloader()
        dlm.download_with = USE_DOWNLOADER
        page = pages.DownloadedPage().set_url('http://httpstat.us/404')
        dlm.download(page)
        self.assertFalse(page.state)
        self.assertEqual(404, page.status_code)

    def test_404_web(self):
        """handling errors"""
        dlm = BaseDownloader()
        dlm.download_with = USE_DOWNLOADER
        page = pages.DownloadedPage().set_url('http://192.155.84.35/scraper/sd')
        dlm.download(page)
        self.assertFalse(page.state)
        self.assertEqual(404, page.status_code)

    def test_timeout_fail(self):
        """handling errors"""
        dlm = BaseDownloader()
        dlm.download_with = USE_DOWNLOADER
        dlm.timeout = 1
        page = pages.DownloadedPage().set_url('http://httpstat.us/524')
        dlm.download(page)
        self.assertFalse(page.state)
        self.assertEqual(524, page.status_code)

    def test_all_codes(self):
        """test with all possible status codes"""
        dlm = BaseDownloader()
        dlm.download_with = USE_DOWNLOADER
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
        dlm.download_with = USE_DOWNLOADER
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
        dlm.download_with = USE_DOWNLOADER
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

#
# {{{
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
# }}}
#


def main():
    """entry point"""
    logger = utils.setup_logger()
    logger.info('### start testing ###')
    unittest.main()


if __name__ == '__main__':
    USE_DOWNLOADER = curl_factory
    # USE_DOWNLOADER = request_factory
    main()
