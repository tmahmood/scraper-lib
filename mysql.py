from config import Config
import MySQLdb
import logging


g_config = Config()
l = '{}.mysql'.format(g_config.g('logger.base'))
logger = logging.getLogger(l)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class MySQL(object):
    """ stores data in a MySQL table """
    def __init__(self):
        super(MySQL, self).__init__()
        self.prep_char = '?'
        self.dbhost = g_config.g('db.mysql.host')
        self.user = g_config.g('db.mysql.user')
        self.pswd = g_config.g('db.mysql.pass')
        self.dbname = g_config.g('db.mysql.database')
        self.connect()

    def connect(self):
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
        # self.db.close()
        return

    def clear_database(self, table):
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
            return self.safe_query(querytpl, data, commit=False)
        conds = []
        fdata = {}
        for item in data:
            col, cond, val = item.split('|', 3)
            conds.append('%s %s=%%(%s)s' % (cond, col, col))
            fdata[col] = val
        querytpl = 'select %s from %s where %s %s' % (cols, table,
                                                      ' '.join(conds), at_end)
<<<<<<< HEAD
        return self.safe_query(querytpl, fdata)
=======
        logger.debug("%s\n%s\n%s", querytpl, data, cols)
        return self.safe_query(querytpl, fdata, commit=False)
>>>>>>> 6a1bb16ee1f9d9c91a410a51123bcb9fed12632d

    def query(self, query):
        cur = self.db.cursor()
<<<<<<< HEAD
        try:
            cur.execute(query)
        except MySQLdb.OperationalError:
            return None
        self.should_commit(query)
=======
        logger.debug(query)
        cur.execute(query)
        if commit:
            self.db.commit()
>>>>>>> 6a1bb16ee1f9d9c91a410a51123bcb9fed12632d
        return cur

    def should_commit(self, _query):
        query = _query.lower()
        insert = query.startswith('insert')
        update = query.startswith('update')
        delete = query.startswith('delete')
        if insert or update or delete:
            self.db.commit()

    def update(self, query, data):
        cur = self.db.cursor()
        status = cur.execute(query, data)
        self.db.commit()
        return status

    def count_rows(self, query):
        res = self.query(query)
<<<<<<< HEAD
        try:
            d = res.fetchone()
            return d[0]
        except Exception:
            return None
=======
        d = res.fetchone()
        return d[0]
>>>>>>> 6a1bb16ee1f9d9c91a410a51123bcb9fed12632d

    def append_data(self, data, table, retries=0):
        qfields = ', '.join(['%%(%s)s' % key for key in data.keys()])
        cols = ', '.join(data.keys())
        q = "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, qfields)
        logger.debug(q)
        logger.debug(data)
<<<<<<< HEAD
        retries = 0
        cur = self.db.cursor()
        while True:
            try:
                status = cur.execute(q, data)
                try:
                    self.lastid = cur.insert_id()
                except Exception as e:
                    self.lastid = cur.lastrowid
                self.db.commit()
                return status
            except MySQLdb.MySQLError as err:
                if err[0] == 1062:
                    return -2
                logger.info('reconnecting ... ')
                self.connect()
                retries += 1
                if retries > 5:
                    logger.exception('Failed to execute query')
                    return None
                continue
            except Exception as e:
                logger.exception('failed inserting data')
                logger.error("%s, %s", table, data)
                self.lastid = None
                raise e
            finally:
                if cur:
                    cur.close()
=======
        cur = self.db.cursor()
        status = cur.execute(q, data)
        if commit:
            self.db.commit()
        try:
            self.lastid = cur.insert_id()
        except Exception:
            self.lastid = cur.lastrowid
        return status
>>>>>>> 6a1bb16ee1f9d9c91a410a51123bcb9fed12632d

    def append_all_data(self, data, table):
        for d in data:
            self.append_data(d, table)


def main():
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
    l = '{}.mysql'.format(g_config.g('logger.base'))
    logger = logging.getLogger(l)
    main()
