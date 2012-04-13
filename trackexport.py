from datetime import datetime
from optparse import OptionParser
from contextlib import contextmanager
from gps.gpslogger import query_tracklog
import logging
import sys
import settings

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from lxml import etree
from lxml.builder import ElementMaker

def _E(default_ns, **kwargs):
    kwargs[None] = default_ns
    return ElementMaker(namespace=default_ns, nsmap=kwargs)

class XML(object):
    EXT_NS = {}

    def __init__(self):
        self.E = _E(self.NAMESPACE, **self.EXT_NS)

    def write(self, f, segs):
        root = self.serialize(segs)
        etree.ElementTree(element=root).write(f, encoding='utf-8', pretty_print=True)

class KML(XML):
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

def process(points, options):
    pass



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

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

    parser = OptionParser(usage='usage: %prog [options] start [end]')
    parser.add_option('-f', '--format', dest='of', default='kml',
                      help='output format (gpx, kml, etc.)')
    parser.add_option('--db', dest='db', default=settings.GPS_LOG_DB,
                      help='tracklog database connector')
    parser.add_option('--style', dest='style', default='fff:1',
                      help='styling (kml only); [color]:[line width]')
    parser.add_option('-a', dest='alt', action='store_true', default=False,
                      help='use true altitude (kml only)')
    parser.add_option('-s', '--simplify', dest='simplify', action='store_true', default=False,
                      help='simplify trackpath by removing redundant points')

    # segment gap threshold
    # max pts per segment
    # straight max dist
    # straight max time
    # straight tolerance
    # clustering removal
    # include alt

    # stoppage markers
    # breadcrumb markers

    # styling


    (options, args) = parser.parse_args()

    try:
        start = parse_timestamp(args[0])
    except IndexError:
        raise Exception('start time required')

    try:
        end = parse_timestamp(args[1])
    except IndexError:
        end = None

    logging.info('exporting %s to %s' % (start, end or '--'))
    with dbsess(options.db) as sess:
        points = list(query_tracklog(sess, start, end))
    logging.debug('%d points fetched' % len(points))

    process(points, options)

    serializer = {
        'gpx': GPX(),
        'kml': KML(true_alt=options.alt, styling=parse_style(options.style)),
    }[options.of]

    serializer.write(sys.stdout, [points])
        


"""

import sys
from datetime import datetime, timedelta
import psycopg2
import geodesy

DB = 'geoloc'
usealt = False

gap_threshold = 3 #s
max_pts_per_segment = 5000

straight_length_max = 300 #m
straight_time_max = 10 #s
straight_threshold = 1. #deg
#straight_tolerance = 10  #m

patches = {

}

def get_points (stdt=None, endt=None):
  conn = psycopg2.connect(database=DB)
  curs = conn.cursor()
  curs.execute('select gps_time, latitude, longitude, altitude from gps_log where gps_time between %(start)s and %(end)s order by gps_time;', dict(start=stdt, end=enddt))

  points = []
  row = curs.fetchone()
  while row != None:
    (timestamp, lat, lon, alt) = row
    points.append((timestamp, lat, lon, alt))
    row = curs.fetchone()
  curs.close()
  conn.close()

  #apply patches

  return points

def segmentize(points):
  seg = []
  lasttime = None
  for p in points:
    if lasttime != None and fdelta(p[0] - lasttime) > gap_threshold:
      yield seg
      seg = []
    seg.append(p)
    lasttime = p[0]
  if len(seg) > 0:
    yield seg

def sub_segmentize(segment):
  subseg = []
  for p in segment:
    subseg.append(p)
    if len(subseg) == max_pts_per_segment:
      yield subseg
      subseg = [p]
  if len(subseg) > 0:
    yield subseg

def get_segments(points):
  return [process_segment(seg) for seg in segmentize(points)]

def fdelta (td):
  return 86400. * td.days + td.seconds + 1.0e-6 * td.microseconds

def dist (p0, p1):
  return geodesy.distance((p0[1], p0[2]), (p1[1], p1[2]))

def bearing (p0, p1):
  return geodesy.bearing((p0[1], p0[2]), (p1[1], p1[2]))

def process_segment(seg):
  seg = process_clustering(seg)
  seg = process_straights(seg)
  return seg

stopped_threshold = [
  (0, 2, 60),
  (30, 5, 30),
  (300, 15, 120),
  (3600, 50, 600)
]

def cluster_bracket (tdiff):
  bracket = 0
  while bracket < len(stopped_threshold) and tdiff >= stopped_threshold[bracket][0]:
    bracket += 1
  bracket -= 1

def process_clustering(seg):
  cseg = []
  bpts = None
  for (i, p) in enumerate(seg):
    if bpts == None:
      include = True
      bpts = [p] * len(stopped_threshold)
    elif i == len(seg) - 1:
      include = True
    else:
      applic = [False] * len(bpts)
      for (j, bp) in enumerate(bpts):
        bracket = stopped_threshold[j]  
        if dist(p, bp) <= bracket[1]:
          applic[j] = fdelta(p[0] - bp[0]) >= bracket[0]
        else:
          bpts[j] = p

      applics = [i for (i, _) in enumerate(applic) if applic[i]]
      max_applic = max(applics) if applics else -1

      if max_applic == -1:
        include = True
      else:
        include = (fdelta(p[0] - cseg[-1][0]) >= stopped_threshold[max_applic][2])


    if include:
      cseg.append(p)

  return cseg

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
  stdt = datetime.strptime(sys.argv[1], '%Y%m%d%H%M')
  enddt = datetime.strptime(sys.argv[2], '%Y%m%d%H%M')
  color = sys.argv[3] if len(sys.argv) > 3 else 'ff0000'

  points = get_points(stdt, enddt)

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
