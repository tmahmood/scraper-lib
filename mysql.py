try:
    from libs.config import Config
    from libs.dbbase import DBBase
except ImportError:
    # pylint: disable=relative-import
    from config import Config
    from dbbase import DBBase
import MySQLdb
import logging
import unittest


def make_columns(data):
    """make columns for data

    :data: @todo
    :returns: @todo

    """
    return ', '.join(['%%(%s)s' % key for key in data.keys()])


def dict_factory(cursor, row):
    """
    dict factory for mysql row
    """
    dest = {}
    for idx, col in enumerate(cursor.description):
        dest[col[0]] = row[idx]
    return dest


class MySQL(DBBase):
    """
    MySQL driver
    """

    logger = None

    """ stores data in a MySQL table """
    def __init__(self, config=None):
        super(MySQL, self).__init__()
        if config == None:
            config = CFG
        self.prep_char = '?'
        self.dbc = None
        self.lastid = None
        self.dbhost = config.g('db.mysql.host')
        self.user = config.g('db.mysql.user')
        self.pswd = config.g('db.mysql.pass')
        self.dbname = config.g('db.mysql.database')
        MySQL.logger = logging.getLogger('{}.mysql'.format(CFG.g('logger.base')))
        self.connect()

    def connect(self):
        """
        connects to database
        """
        try:
            self.dbc.close()
        except AttributeError:
            pass
        self.dbc = MySQLdb.connect(self.dbhost, self.user,
                                   self.pswd, self.dbname, charset='utf8',
                                   use_unicode=True)
        self.dbc.set_character_set('utf8')
        dbc = self.dbc.cursor()
        dbc.execute('SET NAMES utf8;')
        dbc.execute('SET CHARACTER SET utf8;')
        dbc.execute('SET character_set_connection=utf8;')

    def close(self):
        """
        closes the database, don't use it,
        close database directly by self.dbc.close()
        """
        self.dbc.close()

    def clear_database(self, table):
        """
        clears given table
        """
        self.query("delete from %s" % table)

    def safe_query(self, qtpl, data):
        """Executed binding query
        ex: select * from table where q=:s, d=:k

        :query: @todo
        :data: @todo
        :returns: @todo

        """
        retries = 0
        while True:
            try:
                return self.do_query(qtpl, data)
            except MySQLdb.MySQLError as err:
                if err[0] == 1062:
                    return -2
                self.connect()
                retries += 1
                if retries > 5:
                    MySQL.logger.exception('Failed to execute query')
                    return None

    def make_condition(self, cond, col, col_name):
        """builds appropiate query

        :cond: @todo
        :col: @todo
        :col: @todo
        :returns: @todo

        """
        return '%s %s=%%(%s)s' % (cond, col, col_name)

    def query(self, query):
        """
        Runs a query in unsafe way
        """
        try:
            return self._query(query)
        except MySQLdb.OperationalError:
            return None

    def append_data(self, data, table, pkey=None):
        """
        adds row to database
        """
        qfields = make_columns(data)
        cols = ', '.join(data.keys())
        query = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, qfields)
        return self.execute_query(data, query)

    def append_all_data(self, data, table):
        """adds multiple rows,

        tries in single query first
        uses multiple queries if fails
        """
        qfields = make_columns(data[0])
        cols = ', '.join(data[0].keys())
        query = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, qfields)
        state = self.execute_query(data, query, True)
        if state == -2:
            for row in data:
                self.append_data(row, table)
        else:
            return state
        return True

    def execute_query(self, data, query, many=False):
        """execute query

        :data: @todo
        :table: @todo
        :many: @todo
        :returns: @todo

        """
        # pylint: disable=broad-except, no-member
        retries = 0
        cur = None
        try:
            while True:
                try:
                    cur = self.dbc.cursor()
                    if many:
                        status = cur.executemany(query, data)
                    else:
                        status = cur.execute(query, data)
                    try:
                        self.lastid = cur.insert_id()
                    except AttributeError:
                        self.lastid = cur.lastrowid
                    except Exception:
                        self.lastid = cur.lastrowid
                        MySQL.logger.exception("ignorable")
                    self.dbc.commit()
                    return status
                except MySQLdb.MySQLError as err:
                    if err[0] == 1062:
                        return -2
                    MySQL.logger.exception(err)
                    MySQL.logger.info('reconnecting ... ')
                    self.connect()
                    retries += 1
                    if retries > 5:
                        MySQL.logger.exception('Failed to execute query')
                        return None
                    continue
                except Exception as exp:
                    MySQL.logger.exception('failed inserting data')
                    self.lastid = None
                    raise exp
        finally:
            if cur:
                cur.close()


class TestMySQL(unittest.TestCase):
    """docstring for TestMySQL"""

    def test_inserts(self):
        """test insert queries
        :returns: @todo

        """
        dbc.append_data({'name': 'gmail.com', 'si': 10}, 'tests')
        dbc.append_data({'name': 'inbox.com', 'si': 12}, 'tests')
        dbc.append_data({'name': 'reddit.com', 'si': 1}, 'tests')
        dbc.append_data({'name': 'reddit.com', 'si': 2}, 'tests')
        dbc.append_data({'name': 'reddit.com', 'si': 2}, 'tests')
        dbc.query('insert into tests (name, si) values("google.com", 10)')
        self.assertEqual(1, 1)

    def test_queries(self):
        """test select queries
        :returns: @todo

        """
        result = dbc.select('tests', ['name||sgmail.com'])
        self.assertEqual(0, len(result.fetchall()))
        result = dbc.select('tests', ['name||gmail.com'])
        self.assertEqual(1, len(result.fetchall()))
        result = dbc.select('tests', ['si||2', 'si|or|12'])
        self.assertEqual(3, len(result.fetchall()))
        result = dbc.select('tests', ['name||gmail.com', 'name|or|inbox.com'])
        self.assertEqual(2, len(result.fetchall()))
        result = dbc.select('tests', ['name||reddit.com'], 'count(*)')
        self.assertEqual(3, result.fetchone()[0])
        result = dbc.select('tests', at_end='order by si')
        result = dbc.select('tests', ['name||reddit.com'], 'count(*)',
                            at_end='group by si')


def main():
    """
    do some tests
    """
    try:
        dbc.query('drop table if exists tests')
        dbc.query('create table tests(name varchar(20), si integer)')
    # pylint: disable=no-member
    except MySQLdb.OperationalError:
        pass
    unittest.main()


CFG = Config()
if __name__ == '__main__':
    dbc = MySQL()
    main()
