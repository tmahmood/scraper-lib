import unittest
import libs.downloader as downloader
import libs.utils as utils


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
    def test_200(self):
        """test 200 status code"""
        dlm = downloader.BaseDownloader()
        self.assertTrue(dlm.download('http://httpstat.us/200'))
        self.assertEqual(dlm.status_code, 200)

    def test_301(self):
        """redirection
        :returns: @todo

        """
        dlm = downloader.BaseDownloader()
        self.assertTrue(dlm.download('http://httpstat.us/301'))
        self.assertEqual(200, dlm.status_code)
        self.assertEqual(dlm.last_url, 'http://httpstat.us')

    def test_404(self):
        """handling errors"""
        dlm = downloader.BaseDownloader()
        self.assertFalse(dlm.download('http://httpstat.us/404'))
        self.assertEqual(404, dlm.status_code)

    def test_404_web(self):
        """handling errors"""
        dlm = downloader.BaseDownloader()
        self.assertFalse(dlm.download('http://192.155.84.35/scraper/sd'))
        self.assertEqual(404, dlm.status_code)

    def test_timeout_fail(self):
        """handling errors"""
        dlm = downloader.BaseDownloader()
        dlm.timeout = 1
        self.assertFalse(dlm.download('http://httpstat.us/524'))
        self.assertEqual(524, dlm.status_code)

    def test_all_codes(self):
        """test with all possible status codes"""
        dlm = downloader.BaseDownloader()
        for code in CODES:
            info = CODES[code]
            url = 'http://httpstat.us/%s' % code
            self.assertEqual(info[0], dlm.download(url))
            if int(code) >= 400:
                self.assertEqual(int(code), dlm.status_code)

    def test_cached_downloader(self):
        """@todo: Docstring for test_cached_downloader.
        :returns: @todo

        """
        url = 'http://example.com/'
        filename = utils.hash(url)
        fullpath = utils.file_cached_path(filename, url)
        dlm = downloader.CachedDownloader()
        dlm.download(url)
        import os
        self.assertTrue(os.path.exists(fullpath), "cache path exists")


def main():
    unittest.main()
