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

    def write(self, f, segs):
        root = self.serialize(segs)
        etree.ElementTree(element=root).write(f, encoding='utf-8', pretty_print=True)

class KML(XML):
    """serializer for KML format"""

    NAMESPACE = 'http://earth.google.com/kml/2.1'

    def __init__(self, true_alt=False, styling=None):
        super(KML, self).__init__()
        self.true_alt = true_alt
        self.styling = styling

    def serialize(self, segs):
        E = self.E
        return E.kml(
            E.Document(*(self.segment(s) for s in segs))
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
            E.altitudeMode('absolute' if self.true_alt else 'clampToGround'),
            E.coordinates('\n%s\n' % '\n'.join(self.point(p) for p in points))
        ))

        return E.Placemark(*children)

    def point(self, p):
        fmt = '%(lon)f,%(lat)f'
        if p['alt'] is not None:
            fmt += ',%(alt)f'
        return fmt % p

class GPX(XML):
    """serializer for GPX format"""

    NAMESPACE = 'http://www.topografix.com/GPX/1/1'
    EXT_NS = {
        'be': 'http://mrgris.com/schema/birdseye/gpxext/1.0',
    }

    def serialize(self, segs):
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

    def time_diff(p0, p1):
        return p1['time'] - p0['time']

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



straight_length_max = 2000 #m
straight_time_max = 10 #s
straight_threshold = 1. #deg

def simplify_straightaway(seg):
    return seg


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

    segs = remove_redundant(lambda segs: [simplify_straightaway(seg) for seg in segs],
                            lambda segs: sum(len(seg) for seg in segs),
                            'along straightaways', segs)

    if options.max is not None:
        print_('splitting track by max length')
        segs = list(itertools.chain(*(split_max_points(seg, options.max) for seg in segs)))

    return segs

def _ll(p):
    return (p['lat'], p['lon'])

def dist(p0, p1):
    return geodesy.distance(_ll(p0), _ll(p1))

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

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

    parser = OptionParser(usage='usage: %prog [options] start [end]')
    parser.add_option('-f', '--format', dest='of', default='kml',
                      help='output format (gpx, kml, etc.)')
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
    parser.add_option('--stops', dest='stops', default='20:40',
                      help='leave stoppage markers, where position does not move more than X meters for at least Y seconds; [X]:[Y] (kml only)')
    parser.add_option('--no-stops', dest='nostops', action='store_true')
    parser.add_option('--style', dest='style', default='fff:1',
                      help='styling (kml only); [color]:[line width]')
    parser.add_option('-a', dest='alt', action='store_true', default=False,
                      help='use true altitude (kml only)')
    parser.add_option('--max', dest='max', type='int',
                      help='max points per track segment')

    (options, args) = parser.parse_args()

    if options.simplify is None:
        options.simplify = (options.of == 'kml')
    if options.max is None:
        options.max = (5000 if options.of == 'kml' else None)
    if options.of == 'gpx':
        options.nobc = True
        options.nostops = True
    options.bc = [float(k.strip()) for k in options.bc.split(',')] if not options.nobc else []
    options.stops = dict(zip(('dist', 'time'), (float(k) for k in options.stops.split(':')))) if not options.nostops else None

    try:
        start = parse_timestamp(args[0])
    except IndexError:
        raise Exception('start time required')
    try:
        end = parse_timestamp(args[1])
    except IndexError:
        end = None

    print_('exporting [%sZ] to [%sZ]... ' % (start, end or '--'), False)
    with dbsess(options.db) as sess:
        points = list(query_tracklog(sess, start, end))
    print_('%d points fetched' % len(points))

    segments = process_track(points, options)

    serializer = {
        'gpx': GPX(),
        'kml': KML(true_alt=options.alt, styling=parse_style(options.style)),
    }[options.of]

    serializer.write(sys.stdout, segments)
        


"""



def get_segments(points):
  return [process_segment(seg) for seg in segmentize(points)]



def within_tolerance (p0, p1, plast, pbetween):
  if fdelta(p1[0] - p0[0]) > straight_time_max:
    return False
  elif dist(p0, p1) > straight_length_max:
    return False
  
  b01 = bearing(p0, p1)
  b0L = bearing(p0, plast)
  bL1 = bearing(plast, p1)

  if b01 == None or b0L == None or bL1 == None:
    return False

  if abs(geodesy.anglenorm(b0L - b01)) > straight_threshold:
    return False
  elif abs(geodesy.anglenorm(bL1 - b01)) > straight_threshold:
    return False

  #todo: handle tolerance

  return True

#assume distances are small enough that the curvature of the earth is irrelevant
def process_straights(seg):
  sseg = [seg[0]]
  i = 0
  while i < len(seg) - 1:
    j = i + 2
    while j < len(seg):
      if within_tolerance(seg[i], seg[j], seg[j-1], seg[i+1:j-1]):
        j += 1
      else:
        break
    i = j - 1
    sseg.append(seg[i])
  return sseg

def to_kml(stream, segments, bc, stops, color):
  open = ""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://earth.google.com/kml/2.1">
<Document>
""

  startseg = ""<Placemark>
<Style><LineStyle><color>%sff</color><width>2</width></LineStyle></Style>
<LineString>
<tessellate>1</tessellate>
<altitudeMode>%s</altitudeMode>
<coordinates>
"" % (color, 'absolute' if usealt else 'clampToGround')

  endseg = ""</coordinates>
</LineString>
</Placemark>
""

  startpt = ""<Placemark><name>%s</name><Point><coordinates>""
  endpt = ""</coordinates></Point></Placemark>
""

  close = ""</Document>
</kml>
""

  startf = ""<Folder><name>%s</name><open>1</open>
""
  endf = ""</Folder>
""


  stream.write(open)

  stream.write(startf % 'breadcrumbs')
  bcbuck = []
  bcbuck.append(('hourly', [p for p in bc if int(fdelta(p[0] - datetime(2000, 1, 1))) % 3600 == 0]))
  bcbuck.append(('5 minutes', [p for p in bc if int(fdelta(p[0] - datetime(2000, 1, 1))) % 3600 != 0]))
  for (lab, bc) in bcbuck:
    stream.write(startf % lab)
    for p in bc:
      stream.write(startpt % (p[0].strftime('%m-%d %H:%M')))
      print '%s,%s,%s' % (p[2], p[1], 0)
      stream.write(endpt)
    stream.write(endf)
  stream.write(endf)

  stream.write(startf % 'stops')
  stopbuck = []
  stopbuck.append(('over 5 min', [s for s in stops if s[1] >= 300]))
  stopbuck.append(('over 1 min', [s for s in stops if s[1] >= 60 and s[1] < 300]))
  stopbuck.append(('under 1 min', [s for s in stops if s[1] < 60]))
  for (lab, stops) in stopbuck:
    stream.write(startf % lab)
    for stop in stops:
      stream.write(startpt % ('%s - %s' % (stop[0][0].strftime('%m-%d %H:%M'), tlen(stop[1]))))
      print '%s,%s,%s' % (stop[0][2], stop[0][1], 0)
      stream.write(endpt)
    stream.write(endf)
  stream.write(endf)

  stream.write(startf % 'path')
  for seg in segments:
    for s in sub_segmentize(seg):
      stream.write(startseg)
      for p in s:
        print '%s,%s,%s' % (p[2], p[1], p[3] if usealt else 0)
      stream.write(endseg)
  stream.write(endf)

  stream.write(close)

def tlen (i):
  i = int(i)
  s = i % 60
  m = (i / 60) % 60
  h = (i / 3600) % 24
  d = i / 86400

  if d > 0:
    return '%dd%02dh%02dm%02ds' % (d, h, m, s)
  elif h > 0:
    return '%dh%02dm%02ds' % (h, m, s)
  elif m > 0:
    return '%dm%02ds' % (m, s)
  else:
    return '%ds' % (s)

stop_radius = 40
stop_interval = 20

def find_stops(points):
  base = None
  n = None
  stops = []
  for p in points:
    if base == None:
      base = p
      n = 0
    else:
      if dist(p, base) <= stop_radius:
        n = fdelta(p[0] - base[0])
      else:
        if n >= stop_interval:
          stops.append((base, n))
        base = p
        n = 0
  if n >= stop_interval:
    stops.append((base, n))
  return stops

def breadcrumbs(points, interval):
  bcs = []
  data = [(p, int(fdelta(p[0] - datetime(2000, 1, 1)))) for p in points]
  moved = True 
  for i in range(0, len(points)):
    if len(bcs) > 0 and not moved and dist(data[i][0], bcs[-1]) >= 100.:
      moved = True      

    mark = False
    if data[i][1] % interval < 3 and (i == 0 or data[i][1]/interval != data[i-1][1]/interval):
      if moved:
        bcs.append(data[i][0])
        moved = False
    elif i > 0 and fdelta(data[i][0][0] - data[i-1][0][0]) > 5. and data[i][1]/interval != data[i-1][1]/interval and dist(data[i][0], data[i-1][0]) < 100.:
      if moved:
        newpt = (datetime(2000, 1, 1) + timedelta(seconds=(data[i-1][1]/interval + 1)*interval), data[i-1][0][1], data[i-1][0][2], data[i-1][0][3])
        bcs.append(newpt)
        moved = False
        sys.stderr.write(str(newpt) + '\n')

  return bcs

if __name__ == "__main__":
  points = ...

  bc1 = breadcrumbs(points, 300)
  bc2 = breadcrumbs(points, 3600)
  bcm = {}
  for bcd in (bc1, bc2):
    for p in bcd:
      bcm[p[0]] = p
  bc = sorted(bcm.values(), key=lambda x: x[0])

  stops = find_stops(points)
  to_kml(sys.stdout, get_segments(points), bc, stops, color)

"""
