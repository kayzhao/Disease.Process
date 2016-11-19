from collections import defaultdict
from itertools import chain


def list2dict(xlist, sep=":"):
    """
    Convert list of identifiers to a dict, where the key is everything before the first `sep`

    >>> list2dict(['OMIM:1234','OMIM:1234','OMIM:1111','MESH:C009876','GREG:666','SHITTYID:2134:324','malformed_id'])
    {'MESH': ['C009876'], 'SHITTYID': ['2134:324'], 'OMIM': ['1234', '1234', '1111'], None: ['malformed_id'], 'GREG': ['666']}

    :param xlist:
    :param sep:
    :return:
    """
    d = defaultdict(list)
    for item in xlist:
        if sep not in item:
            key, value = None, item
        else:
            key, value = item.split(sep, 1)
        d[key].append(value)
    return dict(d)


def dict2list(d, sep=":"):
    """
    Convert dict of identifiers to a list, where the key is the ID prefix. Joined using `sep`
    Order of returned list is not specified

    >>> dict2list({'MESH': ['C009876'], 'SHITTYID': ['2134:324'], 'OMIM': ['1234', '1234', '1111'], None: ['malformed_id'], 'GREG': ['666']})
    ['OMIM:1234','OMIM:1234','OMIM:1111','MESH:C009876','GREG:666','SHITTYID:2134:324','malformed_id']

    :param d:
    :param sep:
    :return:
    """
    return list(chain(*[[sep.join([k, vv]) if k else vv for vv in v] for k, v in d.items()]))


def timesofar(t0, clock=0, t1=None):
    '''return the string(eg.'3m3.42s') for the passed real time/CPU time so far
       from given t0 (return from t0=time.time() for real time/
       t0=time.clock() for CPU time).'''
    t1 = t1 or time.clock() if clock else time.time()
    t = t1 - t0
    h = int(t / 3600)
    m = int((t % 3600) / 60)
    s = round((t % 3600) % 60, 2)
    t_str = ''
    if h != 0:
        t_str += '%sh' % h
    if m != 0:
        t_str += '%sm' % m
    t_str += '%ss' % s
    return t_str


def ask(prompt, options='YN'):
    '''Prompt Yes or No,return the upper case 'Y' or 'N'.'''
    options = options.upper()
    while 1:
        s = input(prompt + '[%s]' % '|'.join(list(options))).strip().upper()
        if s in options:
            break
    return s

