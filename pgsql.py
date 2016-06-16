try:
    from libs.config import Config
    from libs.dbbase import DBBase
except ImportError:
    # pylint: disable=relative-import
    from config import Config
    from dbbase import DBBase
import psycopg2
import logging
import unittest


def make_columns(data):
    """make columns for data

    :data: dictonary containing column name (key) and value (not used)
    :returns: @todo

    """
    return ', '.join(['%%(%s)s' % key for key in data.keys()])


class PGSql(DBBase):
    """ stores data in a PGSql table """

    logger = None

    def __init__(self, config=None):
        super(PGSql, self).__init__()
        if config == None:
            config = CFG
        self.prep_char = '?'
        self.lastid = None
        self.dbhost = config.g('db.pgsql.host')
        self.user = config.g('db.pgsql.user')
        self.pswd = config.g('db.pgsql.pass')
        self.dbname = config.g('db.pgsql.database')
        PGSql.logger = logging.getLogger('{}.pgsql'.format(CFG.g('logger.base')))
        self.dbc = self.connect()

    def connect(self):
        """
        connects to database
        """
        try:
            return psycopg2.connect(host=self.dbhost, user=self.user,
                                    password=self.pswd, dbname=self.dbname)
        except AttributeError:
            pass

    # pylint: disable=no-self-use
    def close(self):
        """
        closes the database, don't use it,
        close database directly by self.dbc.close()
        """
        raise Exception("You should not use it for postgres")

    def clear_database(self, table):
        """
        clears given table
        """
        self.query("delete from %s" % table)

    def make_condition(self, cond, col, col_name):
        """builds appropiate query

        :cond: @todo
        :col: @todo
        :col: @todo
        :returns: @todo

        """
        return '%s %s=%%(%s)s' % (cond, col, col_name)

    def reconnect(self):
        """reconnects persistant connection
        :returns: @todo

        """
        self.dbc.close()
        self.dbc = self.connect()

    def query(self, query):
        """Runs a query in unsafe way

        """
        try:
            return self._query(query)
        except psycopg2.Error:
            self.reconnect()
            return self.query_new_conn(query)

    def query_new_conn(self, query):
        """query using new connection

        :query: @todo
        :returns: @todo

        """
        try:
            with self.connect() as conn:
                return self._query(query, conn=conn)
        except psycopg2.Error:
            DBBase.logger.exception("failed: %s", query)
            return None

    def safe_query(self, qtpl, data, conn=None):
        """Executed binding query
        ex: select * from table where q=:s, d=:k

        :query: @todo
        :data: @todo
        :returns: @todo

        """
        try:
            return self.do_query(qtpl, data)
        except psycopg2.IntegrityError:
            self.reconnect()
            DBBase.logger.debug("IntegrityError: %s", qtpl)
            return -2
        except psycopg2.Error:
            PGSql.logger.exception('Failed: %s', qtpl)
            return self._safe_query_new_conn(qtpl, data)

    def _safe_query_new_conn(self, qtpl, data):
        """query using new connection
        """
        try:
            with self.connect() as conn:
                return self.do_query(qtpl, data, conn=conn)
        except psycopg2.Error:
            PGSql.logger.exception('Failed: %s', qtpl)
            return self._safe_query_new_conn(qtpl, data)
        return None

    def append_data(self, data, table, pkey='id'):
        """adds row to database

        :data: data to be saved
        :table: name of the table
        :pk: NEED to provide correct pk (primary key) column, to get last insert id
        """
        qfields = make_columns(data)
        cols = ', '.join(data.keys())
        query = "INSERT INTO %s (%s) VALUES (%s) RETURNING %s"\
                % (table, cols, qfields, pkey)
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
            cnt = 0
            for row in data:
                if self.append_data(row, table):
                    cnt += 1
            return cnt
        else:
            return state

    def execute_query(self, data, query, many=False):
        """execute query

        :data: data to be saved
        :table: name of the table
        :many: multiple rows to be inserted or not
        :returns: True or None

        """
        with self.connect() as conn:
            cur = None
            try:
                cur = conn.cursor()
                if many:
                    cur.executemany(query, data)
                else:
                    cur.execute(query, data)
                    self.lastid = cur.fetchone()[0]
                return True
            except psycopg2.IntegrityError:
                PGSql.logger.debug("duplicate %s", query)
                return -2
            except psycopg2.Error:
                PGSql.logger.exception("%s %s", query, data)
        return None


class TestSQLITE(unittest.TestCase):
    """docstring for TestSQLITE"""

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
        # TODO: test for duplicate entries
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
        # NOTE: better to use CREATE SEQUENCE <table>_id_seq than serial
        dbc.query('create table tests(id SERIAL, name varchar(20), si integer)')
    except Exception:
        pass
    unittest.main()


CFG = Config()
if __name__ == '__main__':
    dbc = PGSql()
    main()
