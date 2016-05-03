import sqlite3 as sqlite
try:
    from libs.config import Config
    from libs.dbbase import DBBase
except ImportError:
    from config import Config
    from dbbase import DBBase
import logging
import unittest
import time

g_config = Config()
l = '{}.sqlite'.format(g_config.g('logger.base'))
logger = logging.getLogger(l)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SQLite(DBBase):
    """ stores data in a sqlite table """
    def __init__(self, dbname=None, lazy_commit=False):
        super(SQLite, self).__init__()
        self.dbname = dbname if dbname != None else g_config.g('db.sqlite.file')
        self.timeout = g_config.g('db.sqlite.timeout')
        s = g_config.g('db.sqlite.same_thread', 0)
        if s == 0:
            self.same_thread = False
        else:
            self.same_thread = True
        self.connect()
        self.set_lazy_commit(lazy_commit)

    def set_lazy_commit(self, val):
        """enables lazy_commit
        :returns: @todo

        """
        self.lazy_commit = val
        if self.lazy_commit:
            self.commit_func = self.should_commit_lazy
            self.query_queued = 0
        else:
            self.commit_func = self.should_commit

    def connect(self):
        self.db = sqlite.connect(self.dbname, self.timeout,
                                 check_same_thread=self.same_thread)

    def use_dict(self):
        self.db.row_factory = dict_factory

    def use_tuple(self):
        self.db.row_factory = sqlite.Row

    def close(self):
        self.db.close()

    def clear_database(self, table):
        self.query("delete from %s" % table)

    def safe_query(self, qtpl, data):
        """Executed binding query
        ex: select * from table where q=:s, d=:k

        :query: @todo
        :data: @todo
        :commit: @todo
        :returns: @todo

        """
        try:
            return self.do_query(qtpl, data)
        except sqlite.OperationalError:
            return None

    def make_condition(self, cond, col, col_name):
        """@todo: Docstring for make_condition.

        :cond: @todo
        :col: @todo
        :returns: @todo

        """
        return '%s %s=:%s' % (cond, col, col_name)

    def query(self, query):
        try:
            return self._query(query)
        except sqlite.OperationalError:
            return None

    def count_rows(self, query):
        try:
            res = self.query(query)
            d = res.fetchone()
            return d[0]
        except Exception:
            return 0

    def make_columns(self, data):
        """makes column for sqlite

        :data: @todo
        :returns: @todo

        """
        return ', '.join([':%s' % key for key in data.keys()])

    def append_data(self, data, table):
        """
        add rows to database
        """
        qfields = self.make_columns(data)
        cols = ', '.join(data.keys())
        q = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, qfields)
        return self.execute_query(data, q)

    def execute_query(self, data, query, many=False):
        """executes query

        :data: @todo
        :query: @todo
        :many: @todo
        :returns: @todo

        """
        retries = 0
        cur = None
        self.lastid = None
        try:
            while True:
                try:
                    cur = self.db.cursor()
                    status = cur.execute(query, data)
                    self.commit_func(query)
                    try:
                        self.lastid = cur.insert_id()
                    except Exception as e:
                        self.lastid = cur.lastrowid
                    return status
                except (sqlite.IntegrityError, sqlite.DatabaseError) as sie:
                    logger.debug('IntegrityError %s', sie)
                    return -2
                except sqlite.OperationalError as oie:
                    logger.debug('OperationalError %s', oie)
                    return -3
                except Exception as e:
                    if e[0] == 2006:
                        self.connect()
                        retries += 1
                        if retries < 5:
                            continue
                    logger.exception('failed inserting data')
                    self.lastid = None
                    raise e
        finally:
            if cur:
                cur.close()

    def append_all_data(self, data, table):
        for d in data:
            self.append_data(d, table)
        self.db.commit()
        self.query_queued = 0

    def should_commit_lazy(self, query):
        """override for should_commit

        :query: @todo
        :returns: @todo

        """
        self.query_queued += 1
        if self.query_queued >= 30:
            self.should_commit(query)
            self.query_queued = 0

    def force_commit(self):
        """forces to commit
        :returns: @todo

        """
        self.query_queued = 0
        self.db.commit()


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

    def test_non_lazy_commit(self):
        """test with possible unique data
        :returns: @todo

        """
        db.set_lazy_commit(False)
        start_time = time.clock()
        for k in range(0, 1000):
            db.append_data({'name': 'email_%s.com' % k, 'si': k}, 'uniquetests')
        print(time.clock() - start_time)
        cnt = db.count_rows('select count(*) rows from uniquetests')
        self.assertEqual(1000, cnt)
        db.query('delete from uniquetests')
        db.db.commit()

    def test_lazy_commit(self):
        """test lazy commit

        """
        db.set_lazy_commit(True)
        start_time = time.clock()
        for k in range(0, 1000):
            db.append_data({'name': 'email_%s.com' % k, 'si': k}, 'uniquetests')
        print(time.clock() - start_time)
        cnt = db.count_rows('select count(*) rows from uniquetests')
        self.assertEqual(1000, cnt)


def main():
    try:
        unittest.main()
    except Exception:
        pass

if __name__ == '__main__':
    import os
    if os.path.exists('db'):
        os.unlink('db')
    db = SQLite()
    db.query('create table tests( name test, si integer)')
    db.query('create table uniquetests(name test unique, si integer)')
    main()
    if os.path.exists('db'):
        os.unlink('db')
