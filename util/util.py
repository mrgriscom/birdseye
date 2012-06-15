import time
import collections
from datetime import datetime, timedelta
import operator
import itertools
import random
import settings
import logging
import os.path
import shutil
import tempfile
from contextlib import contextmanager

EPSILON = 1.0e-9

def f_eq(a, b):
    try:
        return abs(a - b) < EPSILON
    except TypeError:
        return a == b

class Interrupted(Exception):
    pass

def wait(delay, abortfunc=lambda: False, increment=0.01):
    """wait until 'delay' seconds have passed; if abortfunc
    returns true, throw exception and terminate immediately;
    abortfunc is checked every 'increment'"""
    end_at = time.time() + delay
    while time.time() < end_at:
        if abortfunc():
            raise Interrupted
        time.sleep(increment)

def wait_until(t):
    """wait until system time 't' is reached"""
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

def fdelta(td):
    return 86400. * td.days + td.seconds + 1.0e-6 * td.microseconds

def to_timestamp(dt):
    return fdelta(dt - datetime.utcfromtimestamp(0.))

def fact_div(a, b):
    """return a! / b!"""
    return product(xrange(b + 1, a + 1)) if a >= b else 1. / fact_div(b, a)

def linear_interp(a, b, k):
    return (1. - k) * a + k * b

def product(n):
    """return the product of a set of numbers

    n -- an iterable of numbers"""
    return reduce(operator.mul, n, 1)

class AggregationIndex(object):
    """an index to efficient compute an aggregation over any subrange
    of an array"""

    def __init__(self, func, data, maxdepth=1):
        """
        func -- aggregation function f(<iterable of values>); must have
          the property f([a,b,c]) == f([a,f([b,c])]) (like max, sum, etc.)
        data -- an array of values
        maxdepth -- how many levels from the bottom to index through
        """

        self.aggfunc = func
        self.data = data
        self.maxdepth = maxdepth

        self.maxstep = 1
        while self.maxstep < len(data):
            self.maxstep *= 2
        self.build_index()

    def build_index(self):
        self.index = {}

        depth = self.maxdepth
        while True:
            step = 2**depth
            if step > self.maxstep:
                break

            for i in range(0, len(self.data), step):
                lo, hi = i, i + step
                self.index[(lo, hi)] = self.aggregate(lo, hi)

            depth += 1

    def aggregate(self, start, end):
        """compute agg(data[start:end])"""

        def values(lo, hi):
            """(lo, hi) must equal 2**n(k, k+1) for some n, k"""
            if lo >= min(end, len(self.data)) or hi <= start:
                return

            x = None
            if start <= lo and end >= hi:
                x = self.index_lookup(lo, hi)

            if x is not None:
                yield x
            else:
                mid = (lo + hi) / 2
                for x in itertools.chain(values(lo, mid), values(mid, hi)):
                    yield x

        return self.aggfunc(values(0, self.maxstep))

    def index_lookup(self, lo, hi):
        if hi - lo == 1:
            return self.data[lo]
        elif hi - lo < 2**self.maxdepth:
            return self.aggfunc(self.data[lo:hi])
        else:
            try:
                return self.index[(lo, hi)]
            except KeyError:
                return None

def to_quadindex(z, x, y, alphabet=None):
    def binary(h, k):
        return [(h / 2**i) % 2 for i in range(k - 1, -1, -1)]

    def to_char(q):
        return alphabet[q] if alphabet is not None else str(q)

    quad = [2 * j + i for i, j in zip(*(binary(h, z) for h in (x, y)))]
    return ''.join(to_char(q) for q in quad)

def from_quadindex(ix, alphabet=None):
    def from_char(c):
        return alphabet.index(c) if alphabet is not None else int(c)

    def unquad(ix):
        for c in ix:
            q = from_char(c)
            yield (q % 2, q / 2)

    def from_binary(v):
        return reduce(lambda a, b: 2 * a + b, v, 0)

    ixsp = zip(*(unquad(ix))) if ix else [[], []]
    x, y = [from_binary(v) for v in ixsp]
    return (len(ix), x, y)

def format_interval(interval, expand=True, max_unit=None, sep='', labels={}, pad=True, colons=False, show_secs=True):
    fields = ['d', 'h', 'm', 's']
    _labels = dict(zip(fields, fields))
    _labels.update(labels)
    labels = _labels

    if colons:
        sep = ':'
        labels = dict((f, '') for f in labels)
        expand=True
        pad = True

    if max_unit:
        expand = True
    if not expand:
        pad = False

    if isinstance(interval, timedelta):
        interval = fdelta(interval)

    t = {
        's': interval % 60,
        'm': (interval // 60) % 60,
        'h': (interval // 3600) % 24,
        'd': interval // 86400,
    }

    min_filled = 's'
    for f in reversed(fields):
        if t[f]:
            min_filled = f
            break
    max_filled = 's'
    for f in fields:
        if t[f]:
            max_filled = f
            break

    if not expand:
        disp = [f for f in fields if t[f]] or ['s']
    else:
        disp = fields[min(fields.index(max_filled), fields.index(max_unit or 's')):]

    if not show_secs:
        disp.remove('s')

    def format_field(f, val, first):
        label = labels[f]
        label = ('s' if val != 1 else '').join(label.split('{s}'))
        return '%s%s' % (('%d' if first or not pad else '%02d') % val, label)

    return sep.join(format_field(f, t[f], i == 0) for i, f in enumerate(disp))

def clip(x, _min=None, _max=None):
    """limit x at min and max, if defined"""
    limit = lambda f, k, lim: f(k, lim) if lim is not None else k
    return limit(min, limit(max, x, _min), _max)

def chunker(it, size):
    chunk = []
    for v in it:
        chunk.append(v)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def set_filter(s, filter, remove=True):
    t = set(e for e in s if filter(e))
    if remove:
        for e in t:
            s.remove(e)
    return t

def rand_elem(s):
    r = random.randint(0, len(s) - 1)
    for i, e in enumerate(s):
        if i == r:
            return e

def manhattan_dist((x0, y0), (x1, y1)):
    return abs(x0 - x1) + abs(y0 - y1)

def layer_property(layer, prop, default=None):
    return (settings.LAYERS.get(layer) or {}).get(prop, default)

def proj_path(*args):
    return os.path.join(settings.PROJECT_ROOT, *args)

def waypoints_path():
    return os.path.expanduser(settings.WAYPOINTS)

def tiles_path():
    return os.path.expanduser(settings.TILE_ROOT)

def pixmap_path(pixmap):
    return proj_path('pixmap', pixmap)

def gps_device(canonical=False):
    """canonical - resolve symlinks"""
    dev = settings.GPS_DEVICE
    if canonical and os.path.exists(dev):
        dev = os.path.realpath(dev)
    return dev

def load_waypoints():
    with open(waypoints_path()) as f:
        return load_waypoints_file(f)

def load_waypoints_file(f):
    def gen_waypoints():
        for ln in f:
            try:
                wpt = parse_waypoint_line(ln.strip())
                if wpt:
                    yield wpt
            except RuntimeError:
                logging.warn('could not parse waypoint line [%s]' % ln.strip())
    waypoints = list(gen_waypoints())

    dups = [k for k, v in map_reduce(waypoints, lambda w: [(w['name'],)], len).iteritems() if v > 1]
    if dups:
        logging.warn('waypoints [%s] are defined more than once' % ', '.join(sorted(dups)))

    return dict((wpt['name'], wpt) for wpt in waypoints)

def parse_waypoint_line(ln):
    def split(s, k):
        return s[:k].strip(), s[k+1:].strip()

    comment = None
    k = ln.find('#')
    if k >= 0:
        ln, comment = split(ln, k)
    if not ln:
        # empty lines and comment-only lines
        return

    k = ln.find(':')
    if k == -1:
        raise RuntimeError()
    name, ln = split(ln, k)
    ll = parse_ll(ln)
    if not ll:
        raise RuntimeError()

    return {'name': name, 'pos': ll, 'comment': comment}

def parse_ll(raw):
    pcs = raw.split(',' if ',' in raw else None)
    try:
        lat = float(pcs[0].strip())
        lon = float(pcs[1].strip())
    except (IndexError, ValueError):
        return None

    if lat < -90. or lat > 90. or lon < -180. or lon > 180.:
        return None

    return (lat, lon)

def save_waypoints(waypoints, new_section_hdr=None, make_backup=True):
    path = waypoints_path()

    if not waypoints:
        return

    with open(path) as f:
        lns = f.readlines()

    for wpt in waypoints:
        wpt['section'] = new_section_hdr
        store_waypoint(wpt, lns)

    if make_backup:
        shutil.copyfile(path, tempfile.mktemp('.wpt.bk', 'birdseye-'))
        
    with open(path, 'w') as f:
        f.writelines(lns)

def store_waypoint(waypoint, lns):
    def writeln(wpt):
        fmt = '%(name)s: %(lat)s %(lon)s'
        if wpt.get('desc'):
            fmt += '  # %(desc)s' 
        fmt += '\n'

        MAX_PREC = 5
        wpt['lat'] = round(wpt['lat'], MAX_PREC)
        wpt['lon'] = round(wpt['lon'], MAX_PREC)

        return fmt % wpt

    def findln(s):
        for i, ln in enumerate(lns):
            if ln.startswith(s):
                return i

    existing_line = None
    if waypoint.get('key'):
        existing_line = findln(waypoint['key'] + ':')

    entry = writeln(waypoint)
    if existing_line is not None:
        lns[existing_line] = entry
    else:
        section = waypoint.get('section')
        if section:
            header = '## %s' % section
            header_line = findln(header)
            if header_line is not None:
                lns.insert(header_line + 1, entry)
            else:
                lns.extend(['\n\n%s\n' % header, entry])
        else:
            lns.append(entry)

@contextmanager
def profile(key='-'):
    start = time.time()
    yield
    end = time.time()
    logging.debug('profiling: %s %.03f' % (key, end - start))

def setup():
    if not os.path.exists(tiles_path()):
        os.makedirs(tiles_path())

    if not os.path.exists(waypoints_path()):
        shutil.copyfile(proj_path('data/waypoints.demo'), waypoints_path())
