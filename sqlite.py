import sqlite3 as sqlite
from config import Config
import logging

g_config = Config()
l = '{}.sqlite'.format(g_config.g('logger.base'))
logger = logging.getLogger(l)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SQLite(object):
    """ stores data in a sqlite table """
    def __init__(self):
        super(SQLite, self).__init__()
        self.prep_char = '?'
        self.dbname = g_config.g('db.sqlite.file')
        self.timeout = g_config.g('db.sqlite.timeout')
        s = g_config.g('db.sqlite.same_thread', 0)
        if s == 0:
            self.same_thread = False
        else:
            self.same_thread = True
        self.connect()

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

    def safe_query(self, qtpl, data, commit=True):
        """Executed binding query
        ex: select * from table where q=:s, d=:k

        :query: @todo
        :data: @todo
        :commit: @todo
        :returns: @todo

        """
        cur = self.db.cursor()
        try:
            cur.execute(qtpl, data)
        except sqlite.OperationalError:
            return None
        if commit:
            try:
                self.db.commit()
            except sqlite.OperationalError:
                pass
        return cur

    def select(self, table, data=[], cols='*', at_end=''):
        """Executes simple select query

        :table: name of the table
        :data: [col|cond|val, ...]
        :cols: name of the columns
        :at_end: if we want order/limit/group
        :returns: cursor

        """
        if len(data) == 0:
            querytpl = 'select %s from %s %s' % (cols, table, at_end)
            logger.debug(querytpl)
            return self.safe_query(querytpl, data)
        conds = []
        fdata = {}
        for item in data:
            col, cond, val = item.split('|', 3)
            conds.append('%s %s=:%s' % (cond, col, col))
            fdata[col] = val
        querytpl = 'select %s from %s where %s %s' % (cols, table,
                                                      ' '.join(conds), at_end)
        logger.debug(querytpl)
        return self.safe_query(querytpl, fdata)

    def query(self, query, commit=True):
        cur = self.db.cursor()
        try:
            cur.execute(query)
        except sqlite.OperationalError:
            return None
        if commit:
            try:
                self.db.commit()
            except sqlite.OperationalError:
                pass
        return cur

    def update(self, query, data, commit=True):
        cur = self.db.cursor()
        status = cur.execute(query, data)
        if commit:
            self.db.commit()
        return status

    def count_rows(self, query):
        res = self.query(query)
        try:
            d = res.fetchone()
            return d[0]
        except Exception:
            return 0

    def append_data(self, data, table, commit=True):
        qfields = ', '.join([':%s' % key for key in data.keys()])
        cols = ', '.join(data.keys())
        q = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, qfields)
        logger.debug(q)
        retries = 0
        while True:
            try:
                cur = self.db.cursor()
                status = cur.execute(q, data)
                if commit:
                    self.db.commit()
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
                logger.error("%s, %s", table, data)
                self.lastid = None
                raise e

    def append_all_data(self, data, table):
        for d in data:
            self.append_data(d, table, commit=False)
        self.db.commit()


def main():
    db = SQLite()
    db.query('create table tests( name test, si integer)')
    db.append_data({'name': 'gmail.com', 'si': 10}, 'tests')
    db.append_data({'name': 'inbox.com', 'si': 12}, 'tests')
    db.append_data({'name': 'reddit.com', 'si': 1}, 'tests')
    db.append_data({'name': 'reddit.com', 'si': 2}, 'tests')
    db.append_data({'name': 'reddit.com', 'si': 2}, 'tests')
    db.query('insert into tests (name, si) values("google.com", 10)')
    result = db.select('tests', ['name||sgmail.com'])
    print(result.fetchall())
    result = db.select('tests', ['name||gmail.com'])
    print(result.fetchall())
    result = db.select('tests', ['si||2', 'si|or|12'])
    print(result.fetchall())
    result = db.select('tests', ['name||gmail.com', 'name|or|inbox.com'])
    print(result.fetchall())
    result = db.select('tests', ['name||reddit.com'], 'count(*)')
    print(result.fetchall())
    result = db.select('tests', at_end='order by si')
    print(result.fetchall())
    result = db.select('tests', ['name||reddit.com'], 'count(*)',
                       at_end='group by si')
    print(result.fetchall())
    db.query('drop table tests')

if __name__ == '__main__':
    main()
