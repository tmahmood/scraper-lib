from libs.config import Config
import MySQLdb
import logging


CFG = Config()
l = '{}.mysql'.format(CFG.g('logger.base'))
logger = logging.getLogger(l)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class MySQL(object):
    """ stores data in a MySQL table """
    def __init__(self, config=None):
        super(MySQL, self).__init__()
        if config == None:
            config = CFG
        self.prep_char = '?'
        self.lastid = None
        self.dbhost = config.g('db.mysql.host')
        self.user = config.g('db.mysql.user')
        self.pswd = config.g('db.mysql.pass')
        self.dbname = config.g('db.mysql.database')
        self.connect()

    def connect(self):
        """
        connects to database
        """
        try:
            self.db.close()
        except AttributeError:
            pass
        self.db = MySQLdb.connect(self.dbhost, self.user,
                                  self.pswd, self.dbname, charset='utf8',
                                  use_unicode=True)
        self.db.set_character_set('utf8')
        dbc = self.db.cursor()
        dbc.execute('SET NAMES utf8;')
        dbc.execute('SET CHARACTER SET utf8;')
        dbc.execute('SET character_set_connection=utf8;')

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

    def safe_query(self, qtpl, data, retries=0):
        """Executed binding query
        ex: select * from table where q=:s, d=:k

        :query: @todo
        :data: @todo
        :returns: @todo

        """
        logger.debug(qtpl)
        logger.debug(data)
        cur = self.db.cursor()
        try:
            cur.execute(qtpl, data)
            self.should_commit(qtpl)
            return cur
        except MySQLdb.MySQLError as err:
            if err[0] == 1062:
                return -2
            self.connect()
            retries += 1
            if retries > 5:
                logger.exception('Failed to execute query')
                return None
            self.safe_query(qtpl, data, retries)

    def select(self, table, data=None, cols='*', at_end=''):
        """Executes simple select query

        :table: name of the table
        :data: [col|cond|val, ...]
        :cols: name of the columns
        :at_end: if we want order/limit/group
        :returns: cursor

        """
        if data is None or len(data) == 0:
            querytpl = 'select %s from %s %s' % (cols, table, at_end)
            logger.debug(querytpl)
            return self.safe_query(querytpl, data)
        conds = []
        fdata = {}
        for item in data:
            col, cond, val = item.split('|', 2) # 2 + 1 splits
            conds.append('%s %s=%%(%s)s' % (cond, col, col))
            fdata[col] = val
        querytpl = 'select %s from %s where %s %s' % (cols, table,
                                                      ' '.join(conds), at_end)
        return self.safe_query(querytpl, fdata)

    def query(self, query):
        """
        Runs a query in unsafe way
        """
        cur = self.db.cursor()
        try:
            cur.execute(query)
        except MySQLdb.OperationalError:
            return None
        self.should_commit(query)
        return cur

    def should_commit(self, _query):
        """
        determine if the query needs to be committed
        """
        query = _query.lower()
        insert = query.startswith('insert')
        update = query.startswith('update')
        delete = query.startswith('delete')
        if insert or update or delete:
            self.db.commit()

    def count_rows(self, query):
        """
        counts row using given query
        """
        res = self.query(query)
        try:
            result = res.fetchone()
            return result[0]
        except Exception:
            return None

    def append_data(self, data, table, retries=0):
        """
        adds row to database
        """
        qfields = ', '.join(['%%(%s)s' % key for key in data.keys()])
        cols = ', '.join(data.keys())
        query = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, qfields)
        return self.execute_query(data, query)

    def append_all_data(self, data, table):
        """
        adds multiple rows
        """
        qfields = ', '.join(['%%(%s)s' % key for key in data[0].keys()])
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
        logger.debug(query)
        retries = 0
        cur = self.db.cursor()
        try:
            while True:
                try:
                    if many:
                        status = cur.executemany(query, data)
                    else:
                        status = cur.execute(query, data)
                    try:
                        self.lastid = cur.insert_id()
                    except Exception:
                        self.lastid = cur.lastrowid
                    self.db.commit()
                    return status
                except MySQLdb.MySQLError as err:
                    if err[0] == 1062:
                        return -2
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
                    self.lastid = None
                    raise exp
        finally:
            if cur:
                cur.close()


def main():
    """
    do some tests
    """
    db = MySQL()
    try:
        db.query('create table tests( name varchar(20), si integer)')
    except MySQLdb.OperationalError:
        pass
    try:
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
        print ("DONE")
    except Exception:
        logger.exception("Failed with error")
        pass
    finally:
        db.query('drop table tests')


if __name__ == '__main__':
    from utils import setup_logger
    base_logger = setup_logger()
    l = '{}.mysql'.format(CFG.g('logger.base'))
    logger = logging.getLogger(l)
    main()
