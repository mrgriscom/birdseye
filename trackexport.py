from datetime import datetime, timedelta
from optparse import OptionParser
from contextlib import contextmanager
from gps.gpslogger import query_tracklog
import logging
import sys
import settings
import util.util as u
import itertools
from bisect import bisect_left, bisect_right
from util import geodesy
import csv
import math

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from lxml import etree
from lxml.builder import ElementMaker

def _E(default_ns, **kwargs):
    kwargs[None] = default_ns
    return ElementMaker(namespace=default_ns, nsmap=kwargs)

class XML(object):
    """abstract parent for XML-format serializers"""

    EXT_NS = {}

    def __init__(self):
        self.E = _E(self.NAMESPACE, **self.EXT_NS)

    def write(self, f, segs, markers):
        root = self.serialize(segs, markers)
        etree.ElementTree(element=root).write(f, encoding='utf-8', pretty_print=True)

class KML(XML):
    """serializer for KML format"""

    NAMESPACE = 'http://earth.google.com/kml/2.1'

    def __init__(self, true_alt=False, styling=None):
        super(KML, self).__init__()
        self.true_alt = true_alt
        self.styling = styling

    def serialize(self, segs, markers):
        E = self.E

        folders = []
        for name, points in markers:
            folders.append((name, (self.waypoint(p, p.get('name')) for p in points)))
        folders.append(('track', (self.segment(s) for s in segs)))

        return E.kml(
            E.Document(
                E.open('1'),
                *(self.folder(*f) for f in folders)
            )
        )
        
    def folder(self, name, content):
        E = self.E

        return E.Folder(
            E.name(name),
            E.visibility('1'),
            E.open('0'),
            *content
        )

    def segment(self, points):
        E = self.E

        children = []
        if self.styling:
            children.append(E.Style(
                E.LineStyle(
                    E.color('ff%s%s%s' % tuple(self.styling['color'][k:k+2] for k in range(4, -2, -2))),
                    E.width(str(self.styling['width']))
                )
            ))
        children.append(E.LineString(
            E.tessellate(str(1)),
            self.altmode(),
            E.coordinates('\n%s\n' % '\n'.join(self.point(p) for p in points))
        ))

        return E.Placemark(*children)

    def waypoint(self, point, name=None):
        E = self.E

        children = []
        if name:
            children.append(E.name(name))
        children.append(E.Point(
            self.altmode(),
            E.coordinates(self.point(point))
        ))

        return E.Placemark(*children)

    def point(self, p):
        fmt = '%(lon)f,%(lat)f'
        if p['alt'] is not None:
            fmt += ',%(alt)f'
        return fmt % p

    def altmode(self):
        return self.E.altitudeMode('absolute' if self.true_alt else 'clampToGround')     

class GPX(XML):
    """serializer for GPX format"""

    NAMESPACE = 'http://www.topografix.com/GPX/1/1'
    EXT_NS = {
        'be': 'http://mrgris.com/schema/birdseye/gpxext/1.0',
    }

    def serialize(self, segs, _):
        E = self.E
        return E.gpx(
            E.trk(*(self.segment(s) for s in segs))
        )
        
    def segment(self, points):
        E = self.E
        return E.trkseg(*(self.point(p) for p in points))

    def point(self, p):
        E = self.E
        EXT = lambda tag, *args, **kwargs: E('{%s}%s' % (self.EXT_NS['be'], tag), *args, **kwargs)

        attr = dict((k, str(p[k])) for k in ('lat', 'lon'))
        children = [
            E.time(p['time'].strftime('%Y-%m-%dT%H:%M:%SZ'))
        ]
        if p['alt'] is not None:
            children.append(E.ele(str(p['alt'])))

        ext_fields = ['speed', 'heading', 'climb', 'h_error', 'v_error']
        def ext_node(field):
            val = p[field]
            if val is not None:
                return EXT(field, str(val))
        ext_nodes = filter(lambda e: e is not None, (ext_node(f) for f in ext_fields))
        if ext_nodes:
            children.append(E.extensions(*ext_nodes))

        return E.trkpt(*children, **attr)

class CSV(object):
    """serializer for CSV format"""

    def __init__(self, header=True):
        self.header = header

    def write(self, f, segs, _):
        fields = ['time', 'lat', 'lon', 'alt', 'speed', 'heading', 'climb', 'h_error', 'v_error', 'segment_id']
        writer = csv.DictWriter(f, fields, extrasaction='ignore')
        if self.header:
            writer.writerow(dict(zip(fields, fields)))
        for i, seg in enumerate(segs):
            for p in seg:
                p['segment_id'] = i + 1
                writer.writerow(p)

def split_time_gap(points, gap_threshold):
    """split the track where the gap between consecutive fixes is more than
    gap_threshold"""
    seg = []
    prev_time = None
    for p in points:
        if prev_time is not None and not p.get('contig') and p['time'] - prev_time > gap_threshold:
            yield seg
            seg = []
        seg.append(p)
        prev_time = p['time']
    if seg:
        yield seg

def split_max_points(seg, max_len):
    """split the track into runs of max_len points"""
    subseg = []
    for p in seg:
        subseg.append(p)
        if len(subseg) == max_len:
            yield subseg
            subseg = [p]
    if len(subseg) > 1 or len(seg) == 1:
        yield subseg

stop_drift_thresholds = [
    {'min_time': 0, 'radius': 2, 'emit_every': 60},
    {'min_time': 30, 'radius': 5, 'emit_every': 30},
    {'min_time': 300, 'radius': 15, 'emit_every': 120},
    {'min_time': 3600, 'radius': 50, 'emit_every': 600},
]

def simplify_stoppage_drift(points, gap_threshold):
    """eliminate redundant points where the track has 'stopped', accounting
    for gps drift"""

    def recent_range_func():
        times = [p['time'] for p in points]
        def f(i, lookback):
            """return the index into 'points' marking the start of the time
            window -- the first point whose time is <= p[i].time - lookback"""
            threshold = points[i]['time'] - timedelta(seconds=max(lookback, 1e-6))
            j = bisect_right(times, threshold)
            return j - 1
        return f
    recent_range = recent_range_func()

    def window_max_gap_func():
        gaps = [time_diff(points[i], points[i + 1]) for i in range(len(points) - 1)]
        ix = u.AggregationIndex(max, gaps, 3)
        def f(istart, iend):
            """return the maximum time gap between fixes in points [istart, iend]"""
            if istart == -1:
                # end of data reached within time window -- treat this as an 'infinite' gap
                return None
            else:
                return ix.aggregate(istart, iend)
        return f
    window_max_gap = window_max_gap_func()

    def within_radius(istart, iend, radius):
        """determine whether all points from [istart,iend) are within radius of iend"""
        # TODO: use a spatial index for this?
        # start with earliest point, as most likely to be farther away
        for i in range(istart, iend):
            if dist(points[i], points[iend]) > radius:
                return False
        return True

    def active_bracket(i, p):
        """determine what 'stoppage bracket' applies to the current point, based on how long
        the track has remained within a certain radius of this point; test each of the brackets,
        favoring the one with the longest emit interval"""

        for bracket in sorted(stop_drift_thresholds, key=lambda b: -b['emit_every']):
            # fetch the look-back window of recent fixes
            i_window_start = recent_range(i, bracket['min_time'])

            # determine if window represents a contiguous block of data -- no gaps between
            # fixes are too long
            max_acceptable_gap = timedelta(seconds=max(bracket['emit_every'], gap_threshold))
            max_gap = window_max_gap(i_window_start, i)
            if max_gap is None or max_gap > max_acceptable_gap:
                continue

            # determine if all recent history is within the required 'radius' of the active point
            if not within_radius(i_window_start, i, bracket['radius']):
                continue
            
            return bracket

    last_point = None
    for i, p in enumerate(points):
        ab = active_bracket(i, p)

        if ab:
            emit = (time_diff(last_point, p) >= timedelta(seconds=ab['emit_every']))
            contig = True
        else:
            emit = True
            # check if the transition from 'stopped' points back to 'moving' points should be contiguous
            contig = None
            if last_point != (points[i - 1] if i > 0 else None):
                # we're here if the last 'stopped' point before the first 'moving' point was eliminated
                contig = (time_diff(points[i - 1], p) <= timedelta(seconds=gap_threshold))

        if emit:
            if contig:
                p['contig'] = True
            yield p
            last_point = p

def simplify_straightaway(seg):
    """eliminate redundant points along a straight path"""
    yield seg[0]
    i = 0
    while i < len(seg) - 1:
        j = i + 2
        while j < len(seg):
            if within_straightness_tolerance(seg[i], seg[j], seg[j-1]):
                j += 1
            else:
                break
        i = j - 1
        yield seg[i]

straight_length_max = 2000 #m
straight_time_max = timedelta(seconds=30)
straight_threshold = 1. #deg

def within_straightness_tolerance (pstart, pend, pmiddle):
    #assume distances are small enough that the curvature of the earth is irrelevant
    if time_diff(pstart, pend) > straight_time_max:
        return False
    elif dist(pstart, pend) > straight_length_max:
        return False
  
    bse = bearing(pstart, pend)
    bsm = bearing(pstart, pmiddle)
    bme = bearing(pmiddle, pend)

    if any(b is None for b in (bse, bsm, bme)):
        return False
    if abs(geodesy.anglenorm(bsm - bse)) > straight_threshold:
        return False
    elif abs(geodesy.anglenorm(bme - bse)) > straight_threshold:
        return False

    return True

def process_track(points, options):
    def remove_redundant(simplifyfunc, countfunc, caption, data):
        if options.simplify:
            before = countfunc(data)
            print_('removing redundant points %s... ' % caption, False)
            data = simplifyfunc(data)
            print_('%d points removed' % (before - countfunc(data)))
        return data

    points = remove_redundant(lambda points: list(simplify_stoppage_drift(points, options.gap)),
                              lambda points: len(points),
                              'when stopped', points)

    print_('splitting track by time gaps... ', False)
    segs = list(split_time_gap(points, timedelta(seconds=options.gap)))
    print_('%d contiguous tracks' % len(segs))

    segs = remove_redundant(lambda segs: [list(simplify_straightaway(seg)) for seg in segs],
                            lambda segs: sum(len(seg) for seg in segs),
                            'along straightaways', segs)

    if options.max is not None:
        print_('splitting tracks by max-points-per-track limit (longest %d points)' % max(len(seg) for seg in segs))
        segs = list(itertools.chain(*(split_max_points(seg, options.max) for seg in segs)))

    return segs

breadcrumb_exclusion_radius = 100. # m

def breadcrumbs(points, interval, gap_threshold):
    last_bc = None
    for i, p in enumerate(points):
        pprev = points[i - 1] if i > 0 else None

        t = u.to_timestamp(p['time'])
        tprev = None
        if pprev is not None:
            # allow interpolation of breadcrumb between points if points close enough in time or distance
            # this handles brief gaps around the breadcrumb point or extended gaps during which we don't move
            if time_diff(pprev, p) <= gap_threshold or dist(p, pprev) <= breadcrumb_exclusion_radius:
                tprev = u.to_timestamp(pprev['time'])

        # determine where between the two points the breadcrumb lies
        # i think this is vulnerable to floating point errors if interval is non-integer
        if t % interval == 0.:
            # common case
            interp = 1.
        elif tprev and (tprev // interval != t // interval):
            # choose the earliest breadcrumb point if range spans multiple (i.e., extended gap)
            target = math.ceil(tprev / interval + 1e-6)
            interp = (interval * target - tprev) / (t - tprev)
        else:
            continue

        if interp == 1.:
            bc = p
        else:
            bc = interpolate_point(pprev, p, interp)
        bc['name'] = str(bc['time'])

        if last_bc is None or dist(last_bc, bc) > breadcrumb_exclusion_radius:
            yield bc
            last_bc = bc

def interpolate_point(pa, pb, k):
    b = bearing(pa, pb)
    d = dist(pa, pb)
    pinterp = geodesy.plot(_ll(pa), b, k * d)[0]
    return {
        'time': datetime.utcfromtimestamp(u.linear_interp(u.to_timestamp(pa['time']), u.to_timestamp(pb['time']), k)),
        'lat': pinterp[0],
        'lon': pinterp[1],
        'alt': u.linear_interp(pa['alt'], pb['alt'], k) if all(p['alt'] is not None for p in (pa, pb)) else None,
    }
    
def stopped_markers(points, radius, threshold):
    # this algorithm feels sub-optimal; also, the radius is treated more
    # like a diameter

    def mk_stoppage(stopped):
        duration = time_diff(stopped[0], stopped[-1])
        tmid = stopped[0]['time'] + timedelta(seconds=0.5 * u.fdelta(duration))
        pmid = stopped[bisect_left([p['time'] for p in stopped], tmid)]
        pmid.update({'duration': duration, 'name': u.format_interval(duration), 'alt': None})
        return pmid

    def stoppage(i, j):
        if time_diff(points[i], points[j - 1]) >= threshold:
            yield mk_stoppage(points[i:j])

    i_base = 0
    for i, p in enumerate(points):
        if dist(p, points[i_base]) > radius:
            for s in stoppage(i_base, i):
                yield s
            i_base = i
    for s in stoppage(i_base, len(points)):
        yield s

def process_markers(points, options):
    # breadcrumbs
    for interval in sorted(options.bc, reverse=True):
        interval_name = u.format_interval(interval, expand=False)
        print_('marking breadcrumbs at interval %s' % interval_name)
        bcs = list(breadcrumbs(points, u.fdelta(interval), timedelta(seconds=options.gap)))
        yield ('breadcrumbs: %s' % interval_name, bcs)

    # stops
    if options.stops:
        print_('marking where stopped for at least %(time)d seconds (%(dist)dm radius)' % options.stops)
        stops = stopped_markers(points, options.stops['dist'], timedelta(seconds=options.stops['time']))

        brackets = [timedelta(minutes=m) for m in [1, 5, 30, 180]]
        bucketed = u.map_reduce(stops, lambda s: [(bisect_right(brackets, s['duration']), s)], lambda v: sorted(v, key=lambda s: s['time']))
        def bucket_name(i):
            if i < len(brackets):
                return '<%s' % u.format_interval(brackets[i], expand=False)
            else:
                return '>%s' % u.format_interval(brackets[-1], expand=False)

        for i, v in sorted(bucketed.items(), reverse=True):
            yield ('stopped: %s' % bucket_name(i), v)

def _ll(p):
    return (p['lat'], p['lon'])

def dist(p0, p1):
    return geodesy.distance(_ll(p0), _ll(p1))

def bearing(p0, p1):
    return geodesy.bearing(_ll(p0), _ll(p1))

def time_diff(p0, p1):
    return p1['time'] - p0['time']

@contextmanager
def dbsess(conn):
    e = create_engine(conn)
    sess = sessionmaker(bind=e)()
    yield sess
    sess.close()

def parse_timestamp(s):
    s = (s + '000000')[:14]
    return datetime.strptime(s, '%Y%m%d%H%M%S')

def parse_style(sty):
    color, width = sty.split(':')
    if len(color) == 3:
        color = ''.join(''.join(x) for x in zip(color, color))
    return {'color': color, 'width': int(width)}

def print_(text, newline=True):
    sys.stderr.write(str(text) + ('\n' if newline else ''))

def format_type(format):
    return {
        'kml': 'visual',
        'gpx': 'raw',
        'csv': 'raw',
    }[format]

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

    parser = OptionParser(usage='usage: %prog [options] start [end]')
    parser.add_option('-o', '--of', '--format', dest='of', default='kml',
                      help='output format (gpx, kml, csv, etc.)')
    parser.add_option('--db', dest='db', default=settings.GPS_LOG_DB,
                      help='tracklog database connector')
    parser.add_option('-g', '--gap', dest='gap', type='int', default=3,
                      help='maximum gap between fixes before starting new track segment (seconds)')
    parser.add_option('-s', '--simplify', dest='simplify', action='store_true',
                      help='simplify trackpath by removing redundant points')
    parser.add_option('--no-simplify', dest='simplify', action='store_false')
    parser.add_option('--bc', '--breadcrumbs', dest='bc', default='5,60',
                      help='leave breadcrumb markers (comma-separated list of intervals (minutes)) (kml only)')
    parser.add_option('--no-bc', dest='nobc', action='store_true',
                      help='disable breadcrumb markers')
    parser.add_option('--stops', dest='stops', default='40:20',
                      help='leave stoppage markers, where position does not move more than X meters for at least Y seconds; [X]:[Y] (kml only)')
    parser.add_option('--no-stops', dest='nostops', action='store_true')
    parser.add_option('--style', dest='style', default='f40:2',
                      help='styling (kml only); [color]:[line width]')
    parser.add_option('-a', dest='alt', action='store_true', default=False,
                      help='use true altitude (kml only)')
    parser.add_option('--max', dest='max', type='int',
                      help='max points per track segment')

    (options, args) = parser.parse_args()

    if options.simplify is None:
        options.simplify = (format_type(options.of) == 'visual')
    if options.max is None:
        options.max = (5000 if options.of == 'kml' else None)
    if format_type(options.of) == 'raw':
        options.nobc = True
        options.nostops = True
    options.bc = [timedelta(minutes=float(k.strip())) for k in options.bc.split(',')] if not options.nobc else []
    options.stops = dict(zip(('dist', 'time'), (float(k) for k in options.stops.split(':')))) if not options.nostops else None

    try:
        start = parse_timestamp(args[0])
    except IndexError:
        raise Exception('start time required')
    try:
        end = parse_timestamp(args[1])
    except IndexError:
        end = None

    print_('exporting to %s' % options.of)
    if end:
        print_('fetching [%sZ] to [%sZ] (span: %s)... ' % (start, end, u.format_interval(end - start, expand=False, sep=' ')), False)
    else:
        print_('fetching [%sZ] to end of log... ' % start, False)
    with dbsess(options.db) as sess:
        points = list(query_tracklog(sess, start, end))
    print_('%d points fetched' % len(points))

    markers = list(process_markers(points, options))
    segments = process_track(points, options)

    serializer = {
        'gpx': GPX(),
        'kml': KML(true_alt=options.alt, styling=parse_style(options.style)),
        'csv': CSV(),
    }[options.of]

    print_('writing...')
    serializer.write(sys.stdout, segments, markers)

