import logging
from lxml.etree import XMLSyntaxError
from lxml.html.clean import Cleaner
from lxml import html
try:
    import libs.config as config
    import libs.utils as utils
except ImportError:
    import config
    import utils


def clean_dom(dom):
    """get rids of script, style and comments"""
    cleaner = Cleaner()
    cleaner.script = True
    cleaner.style = True
    cleaner.comments = True
    return cleaner.clean_html(dom)


def load_dom(content, remove_br):
    """loads the content

    :content: html
    :remove_br: should remove <br> tags?
    :returns: dom

    """
    if remove_br:
        content = utils.remove_br(content)
    dom = html.fromstring(content)
    return Dom(dom)


# pylint: disable=too-few-public-methods
class Logger(object):
    """logger class"""

    def __init__(self):
        super(Logger, self).__init__()
        self.cfg = config.Config()
        txt = '{}.dm'.format(self.cfg.g('logger.base'))
        self.log = logging.getLogger(txt)


class BasePage(Logger):
    """result of downloads are stored here"""
    def __init__(self):
        super(BasePage, self).__init__()
        self.url = None
        self.post = None
        self.state = False
        self.logger = None
        self.load_time = None

    def set_url(self, url):
        """set url and logger is set based on url

        :url: @todo
        :returns: @todo

        """
        self.url = url
        if self.logger is None:
            self.set_logger()
        self.set_logger()
        return self

    def set_logger(self):
        """set up logger"""
        netloc = utils.get_shorted_url(self.url)
        txt = '{}.page:{}'.format(self.cfg.g('logger.base'), netloc)
        self.logger = logging.getLogger(txt)
        return self

    def set_post(self, post):
        """set post

        :url: @todo
        :returns: @todo

        """
        self.post = post
        return self

    def set_load_time(self, load_time):
        """sets time took to load the page"""
        self.load_time = load_time
        return self


class DownloadedPage(BasePage):
    """store page Information"""
    def __init__(self):
        super(DownloadedPage, self).__init__()
        self.url = None
        self.post = None
        self.last_url = None
        self.status_code = None
        self.text = None
        self.raw_text = None
        self.dom = None

    def get_dom(self, remove_br=False):
        """returns dom"""
        content = self.text
        tried_non_unicode = False
        while True:
            try:
                return load_dom(content, remove_br)
            except ValueError:
                if tried_non_unicode is True:
                    self.log.exception("failed to unicode: %s", self.url)
                    break
                tried_non_unicode = True
                content = self.raw_text
            except XMLSyntaxError:
                self.log.exception('failed to load dom: %s', self.url)
                break
        return None

    def set_last_url(self, last_url):
        """set last url set in response, is useful for redirected webpages"""
        self.last_url = last_url
        return self

    def set_status_code(self, status_code):
        """sets status code

        :status_code: @todo
        :returns: @todo

        """
        self.status_code = status_code
        self.state = self.status_code < 400
        return self

    def set_state(self, state):
        """set state

        :state: @todo
        :returns: @todo

        """
        self.state = state
        return self

    def set_text(self, text, raw_text=None):
        """set text values

        :text: @todo
        :returns: @todo

        """
        self.text = text
        self.raw_text = raw_text
        return self


class Dom(object):
    """dom helper,

    incase we have to switch to beautifulsoup parser
    """

    def __init__(self, dom):
        super(Dom, self).__init__()
        self.dom = dom

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
            return elm.attrib[attr]
        except (KeyError, IndexError, AttributeError):
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
        return self.dom.xpath(xpath)

    def make_links_absolute(self, link):
        """calls make_links_absolute
        :returns: @todo

        """
        self.dom.make_links_absolute(link)
