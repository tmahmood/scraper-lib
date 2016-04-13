
class DBBase(object):

    """base database object"""

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

    def do_query(self, qtpl, data):
        """execute query

        :qtpl: @todo
        :data: @todo
        :returns: @todo

        """
        cur = self.db.cursor()
        cur.execute(qtpl, data)
        self.should_commit(qtpl)
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
            return self.safe_query(querytpl, data)
        conds = []
        fdata = {}
        for k, item in enumerate(data):
            col, cond, val = item.split('|', 3)
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
        cur = self.db.cursor()
        cur.execute(query)
        self.should_commit(query)
        return cur

    def count_rows(self, query):
        """
        counts row using given query
        """
        try:
            res = self.query(query)
            result = res.fetchone()
            return result[0]
        except Exception:
            return None
