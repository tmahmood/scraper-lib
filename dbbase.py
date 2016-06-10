"""
base database stuffs
"""
import logging
from config import Config

G_CFG = Config()


class DBBase(object):
    """base database object"""

    logger = logging.getLogger('{}.dbbase'.format(G_CFG.g('logger.base')))

    def __init__(self):
        """
        initiate common requirements

        """
        self.dbc = None

    def should_commit(self, _query):
        """
        determine if the query needs to be committed
        """
        query = _query.lower()
        insert = query.startswith('insert')
        update = query.startswith('update')
        delete = query.startswith('delete')
        if insert or update or delete:
            self.dbc.commit()

    def do_query(self, qtpl, data):
        """execute query

        :qtpl: @todo
        :data: @todo
        :returns: @todo

        """
        cur = self.dbc.cursor()
        cur.execute(qtpl, data)
        self.should_commit(qtpl)
        return cur

    def make_condition(self, cond, col, col_name):
        """method signature

        :cond: @todo
        :col: @todo
        :col_name: @todo
        :returns: @todo

        """
        raise NotImplementedError()

    def safe_query(self, querytpl, data):
        """method signature

        :querytpl: @todo
        :data: @todo
        :returns: @todo

        """
        raise NotImplementedError()

    def query(self, query):
        """method signature

        :querytpl: @todo
        :returns: @todo

        """
        raise NotImplementedError()

    def select(self, table, data=None, cols='*', at_end=''):
        """Executes simple select query

        :table: name of the table
        :data: [col|cond|val, ...]
        :cols: name of the columns
        :at_end: if we want order/limit/group
        :returns: cursor

        """
        if data == None:
            querytpl = 'select %s from %s %s' % (cols, table, at_end)
            return self.safe_query(querytpl, data)
        conds = []
        fdata = {}
        for k, item in enumerate(data):
            try:
                col, cond, val = item.split('|', 3)
            except ValueError:
                breaks = item.split('|')
                col = breaks[0]
                cond = breaks[1]
                val = '|'.join(breaks[2:])
            col_name = '%s_%s' % (col, k)
            fdata[col_name] = val
            conds.append(self.make_condition(cond, col, col_name))
        querytpl = 'select %s from %s where %s %s' % (cols, table,
                                                      ' '.join(conds), at_end)
        return self.safe_query(querytpl, fdata)

    def _query(self, query):
        """runs query

        :query: @todo
        :returns: @todo

        """
        cur = self.dbc.cursor()
        cur.execute(query)
        self.should_commit(query)
        return cur

    def count_rows(self, query):
        """
        counts row using given query
        """
        res = self.query(query)
        result = res.fetchone()
        return result[0]
