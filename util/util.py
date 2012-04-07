import time
import collections

EPSILON = 1.0e-9

def f_eq(a, b):
    try:
        return abs(a - b) < EPSILON
    except TypeError:
        return a == b

class Interrupted(Exception):
    pass

def wait(delay, abortfunc=lambda: False, increment=0.01):
    end_at = time.time() + delay
    while time.time() < end_at:
        if abortfunc():
            raise Interrupted
        time.sleep(increment)

def wait_until(t):
    try:
        wait(1e8, lambda: time.time() >= t)
    except Interrupted:
        pass

class Acquirer(object):
    def __init__(self, get, retry_interval, exs=Exception):
        self.get = get
        self.retry_interval = retry_interval
        self.exs = tuple(exs if hasattr(exs, '__iter__') else [exs])

        self.retry_at = None

    def acquire(self, abortfunc=lambda: False, immed=True):
        def delay():
            self.retry_at = time.time() + self.retry_interval
            wait(self.retry_interval, abortfunc)

        if not immed:
            delay()

        while True:
            try:
                self.retry_at = None
                return self.get()
            except self.exs:
                delay()

def map_reduce(data, emitfunc=lambda rec: [(rec,)], reducefunc=lambda v: v):
    """perform a "map-reduce" on the data

    emitfunc(datum): return an iterable of key-value pairings as (key, value). alternatively, may
        simply emit (key,) (useful for reducefunc=len)
    reducefunc(values): applied to each list of values with the same key; defaults to just
        returning the list
    data: iterable of data to operate on
    """
    mapped = collections.defaultdict(list)
    for rec in data:
        for emission in emitfunc(rec):
            try:
                k, v = emission
            except ValueError:
                k, v = emission[0], None
            mapped[k].append(v)
    return dict((k, reducefunc(v)) for k, v in mapped.iteritems())

def try_import(path):
    steps = path.split('.')
    module = '.'.join(steps[:-1])
    attr = steps[-1]

    return getattr(__import__(module, fromlist=[attr]), attr)
