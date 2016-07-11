import json


# {{{ Messages
class Message(object):
    """sends message and things"""
    def __init__(self, conn):
        super(Message, self).__init__()
        self.conn = conn

    def send_msg(self, msg, close=True):
        """
        send a message through socket
        """
        try:
            msg = json.dumps(msg)
        except Exception:
            pass
        replylen = len(msg)
        msg = "%s\n%s" % (replylen, msg)
        self.conn.send(bytearray(msg, 'utf8'))
        if close:
            self.conn.close()

    def send_good_msg(self, msg):
        """
        send success messages
        """
        self.send_msg({'s': 1, 'm': msg})

    def send_fail_msg(self, msg):
        """
        send fail messages
        """
        self.send_msg({'s': 0, 'm': msg})

    def send_good_result(self, data):
        """
        sent result when success
        """
        data['s'] = 1
        self.send_msg(data)

    def send_fail_result(self, data):
        """
        send result with fail
        """
        data['s'] = 0
        self.send_msg(data)

# }}}
