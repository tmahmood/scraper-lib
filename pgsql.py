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

    cfg = None
    logger = None

    def __init__(self):
        super(PGSql, self).__init__()
        PGSql.cfg = Config()
        txt = '{}.pgsql'.format(PGSql.cfg.g('logger.base'))
        PGSql.logger = logging.getLogger(txt)
        self.prep_char = '?'
        self.lastid = None
        self.dbhost = PGSql.cfg.g('db.pgsql.host')
        self.user = PGSql.cfg.g('db.pgsql.user')
        self.pswd = PGSql.cfg.g('db.pgsql.pass')
        self.dbname = PGSql.cfg.g('db.pgsql.database')
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
        self.dbc.close()

    def clear_database(self, table):
        """
        clears given table
        """
        self.query("delete from %s" % table)
        self.dbc.commit()

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
        PGSql.logger.info("reconnecting")
        self.dbc.close()
        self.dbc = self.connect()

    def query(self, query):
        """Runs a query in unsafe way
        """
        try:
            if self.requires_commit(query) is False:
                return self._query(query)
            with self.connect() as conn:
                return self._query(query, conn=conn)
        except psycopg2.Error:
            return None

    def safe_query(self, qtpl, data, conn=None, retries=0):
        """Executed binding query
        ex: select * from table where q=%s, d=%s

        :query: @todo
        :data: @todo
        :returns: @todo

        """
        try:
            if self.requires_commit(qtpl) is False:
                return self.do_query(qtpl, data)
            with self.connect() as conn:
                return self.do_query(qtpl, data, conn=conn)
        except psycopg2.IntegrityError:
            self._query('rollback')
            PGSql.logger.debug("IntegrityError: %s", qtpl)
            return -2
        except (psycopg2.InterfaceError, psycopg2.OperationalError,
                psycopg2.DatabaseError):
            PGSql.logger.debug('closed, reconnecting')
            self.reconnect()
            retries += 1
            if retries > 5:
                PGSql.logger.exception("Failed to execute_query")
                return None
            self.safe_query(qtpl, data, conn, retries=retries)
        except psycopg2.Error:
            PGSql.logger.exception('Failed: %s', qtpl)
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
        if state == -2 or state == -3:
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
            except psycopg2.IntegrityError as iexp:
                PGSql.logger.debug("duplicate %s %s", query, iexp)
                return -2
            except psycopg2.DataError as err:
                PGSql.logger.debug("data error %s, %s", query, err)
                return -3
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


if __name__ == '__main__':
    dbc = PGSql()
    main()
