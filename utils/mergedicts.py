__author__ = 'kayzhao'

import time


def f1(d1, d2):
    return dict(d1, **d2)


def f2(d1, d2):
    # python2 use +
    # return dict(d1.items() +  d2.items())
    # python2 use |
    return dict(d1.items() | d2.items())


def f3(d1, d2):
    d = d1.copy()
    d.update(d2)
    return d


def f4(d1, d2):
    d1.update(d2)
    return d1


def f5(d1, d2):
    d = dict(d1)
    d.update(d2)
    return d


def f6(d1, d2):
    return (lambda a, b: (lambda a_copy: a_copy.update(b) or a_copy)(a.copy()))(d1, d2)


def f7(d1, d2):
    d = {}
    d.update(d1)
    d.update(d2)
    return d


def t(f, n):
    st = time.time()
    for i in range(1000000):
        dic1 = {'a': 'AA', 'b': 'BB', 'c': 'CC'}
        dic2 = {'A': 'aa', 'B': 'bb', 'C': 'cc'}
        f(dic1, dic2)
    et = time.time()
    print('%s cost:%s' % (n, et - st))


if __name__ == '__main__':
    t(f1, 'f1')
    t(f2, 'f2')
    t(f3, 'f3')
    t(f4, 'f4')
    t(f5, 'f5')
    t(f6, 'f6')
    t(f7, 'f7')



