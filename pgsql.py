try:
    from libs.config import Config
    from libs.dbbase import DBBase
except ImportError:
    from config import Config
    from dbbase import DBBase
import psycopg2
import logging
import unittest


class PGSql(DBBase):
    """ stores data in a PGSql table """
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
        self.connect()

    def connect(self):
        """
        connects to database
        """
        try:
            self.db.close()
        except AttributeError:
            pass
        self.db = psycopg2.connect(host=self.dbhost, user=self.user,
                                  password=self.pswd, dbname=self.dbname)

    def close(self):
        """
        closes the database, don't use it,
        close database directly by self.db.close()
        """
        self.db.close()

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
            except Exception as err:
                if err[0] == 1062:
                    return -2
                self.connect()
                retries += 1
                if retries > 5:
                    logger.exception('Failed to execute query')
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
        """Runs a query in unsafe way

        """
        try:
            return self._query(query)
        except Exception:
            return None

    def make_columns(self, data):
        """make columns for data

        :data: dictonary containing column name (key) and value (not used)
        :returns: @todo

        """
        return ', '.join(['%%(%s)s' % key for key in data.keys()])

    def append_data(self, data, table, pk='id'):
        """adds row to database

        :data: data to be saved
        :table: name of the table
        :pk: NEED to provide correct pk (primary key) column, to get last insert id
        """
        qfields = self.make_columns(data)
        cols = ', '.join(data.keys())
        query = "INSERT INTO %s (%s) VALUES (%s) RETURNING %s"\
                % (table, cols, qfields, pk)
        status = self.execute_query(data, query, table=table)
        if status == None:
            return None

    def append_all_data(self, data, table):
        """adds multiple rows,

        tries in single query first
        uses multiple queries if fails
        """
        qfields = self.make_columns(data[0])
        cols = ', '.join(data[0].keys())
        query = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, qfields)
        state = self.execute_query(data, query, True, table=table)
        if state == -2:
            for row in data:
                self.append_data(row, table)
        else:
            return state
        return True

    def execute_query(self, data, query, many=False, table=None):
        """execute query

        :data: data to be saved
        :table: name of the table
        :many: multiple rows to be inserted or not
        :returns: True or None

        """
        retries = 0
        cur = None
        try:
            while True:
                try:
                    cur = self.db.cursor()
                    if many:
                        cur.executemany(query, data)
                    else:
                        cur.execute(query, data)
                        self.lastid = cur.fetchone()[0]
                    self.db.commit()
                    return True
                except Exception as err:
                    # TODO: Better error handling
                    # TODO: Duplicate error handle
                    logger.exception(err)
                    logger.info('reconnecting ... ')
                    self.connect()
                    retries += 1
                    if retries > 5:
                        logger.exception('Failed to execute query')
                        return None
                    continue
                except Exception as exp:
                    logger.exception('failed inserting data')
                    raise exp
        finally:
            if cur:
                cur.close()
        return None

# NOTE: should not depend on ordering of tests


class TestSQLITE(unittest.TestCase):
    """docstring for TestSQLITE"""

    def test_inserts(self):
        """test insert queries
        :returns: @todo

        """
        global db
        db.append_data({'name': 'gmail.com', 'si': 10}, 'tests')
        db.append_data({'name': 'inbox.com', 'si': 12}, 'tests')
        db.append_data({'name': 'reddit.com', 'si': 1}, 'tests')
        db.append_data({'name': 'reddit.com', 'si': 2}, 'tests')
        db.append_data({'name': 'reddit.com', 'si': 2}, 'tests')
        db.query('insert into tests (name, si) values("google.com", 10)')

    def test_queries(self):
        """test select queries
        :returns: @todo

        """
        # TODO: test for duplicate entries
        global db
        result = db.select('tests', ['name||sgmail.com'])
        self.assertEqual(0, len(result.fetchall()))
        result = db.select('tests', ['name||gmail.com'])
        self.assertEqual(1, len(result.fetchall()))
        result = db.select('tests', ['si||2', 'si|or|12'])
        self.assertEqual(3, len(result.fetchall()))
        result = db.select('tests', ['name||gmail.com', 'name|or|inbox.com'])
        self.assertEqual(2, len(result.fetchall()))
        result = db.select('tests', ['name||reddit.com'], 'count(*)')
        self.assertEqual(3, result.fetchone()[0])
        result = db.select('tests', at_end='order by si')
        result = db.select('tests', ['name||reddit.com'], 'count(*)',
                           at_end='group by si')


def main():
    """
    do some tests
    """
    try:
        db.query('drop table if exists tests')
        # NOTE: better to use CREATE SEQUENCE <table>_id_seq than serial
        db.query('create table tests(id SERIAL, name varchar(20), si integer)')
    except Exception:
        pass
    unittest.main()


CFG = Config()
if __name__ == '__main__':
    from utils import setup_logger
    base_logger = setup_logger()
    l = '{}.pgsql'.format(CFG.g('logger.base'))
    logger = logging.getLogger(l)
    db = PGSql()
    main()
else:
    l = '{}.pgsql'.format(CFG.g('logger.base'))
    logger = logging.getLogger(l)
