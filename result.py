
def _rst(r, k='s', v=None):
    if v == None: return result[k]
    result[k] = v

def s(result, v=None):
    return _rst(result, 's', v)

def m(result, v=None):
    return _rst(result, 'match', v)

def o(result, v=None):
    return _rst(result, 'original', v)

def b(result, v=None):
    return _rst(result, 'by', v)

def i(result, v=None):
    return _rst(result, 'info', v)

def gt(m, o, i):
    return { "s": True, "match": m, "original":o, "info":i, "by": 'title' }

def gc(m, o, i):
    return { "s": True, "match": m, "original":o, "info":i, "by": 'content' }

def gl(m, o, i):
    return { "s": True, "match": m, "original":o, "info":i, "by": 'link' }

def gf(o):
    return { "s": False, "original":o }
