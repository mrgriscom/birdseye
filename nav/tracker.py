import threading
import os
import time
from util import geodesy
from gps import gpslistener
from gps.gpslogger import query_tracklog
import sys
from datetime import datetime, timedelta
import util.util as u
import Queue
import settings
import logging
import math
import numpy as np
from numpy.linalg import solve

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Tracker(threading.Thread):
    """listen to a stream of position fixes and provide a smooth, interpolated
    trackpath"""

    def __init__(self, fixstream, vector_mode='raz'):
        threading.Thread.__init__(self)
        self.daemon = True
        self.lock = threading.Lock()

        self.fixstream = fixstream
        self.vector_mode = vector_mode

        self.fixbuffer = []
        self.interpolants = None

    def run(self):
        for fix in self.fixstream:
            if self.filter_fix(fix):
                self.update(fix)

    def filter_fix(self, fix):
        """determine if fix is of sufficient quality to use"""
        # could check position error against a threshold, etc.
        # for now, accept all
        return True

    def update(self, fix):
        """update the current track parameters based on new fix"""

        # note: the timestamp of incoming fixes is ignored; they are
        # considered to be effective immediately. this is no concept of
        # a new fix being out-of-date by some delay in processing.
        # however, timestamps are considered to computing the relative
        # time difference among recent fixes

        with self.lock:
            self.add_fix(fix)
            self.interpolate()

    def add_fix(self, fix):
        """update the set of fixes to be used for track interpolation"""
        self.fixbuffer.insert(0, fix)
        # keep most-recent 4 fixes
        self.fixbuffer = self.fixbuffer[:4]

    def transform(self):
        """convert buffered fixes to xyz coordinate system"""
        return [to_xyzt(f, self.fixbuffer[0]) for f in self.fixbuffer]

    def interpolate(self):
        """compute and update interpolation factors"""
        coords = self.transform()
        self.interpolants = dict((axis, interpolate(axis, data)) for axis, data in split_by_axis(coords))
        self.interpolants['t0'] = self.fixbuffer[0]['time']

    def get_loc(self):
        """compute instantaneous location based on current track parameters"""
        with self.lock:
            if self.interpolants is None:
                return None

            # evaluate interpolation function at specified time t
            dt = u.fdelta(datetime.utcnow() - self.interpolants['t0'])
            motion = dict(zip(('p', 'v', 'a', 'j'), zip(*(self.interpolants[axis](dt) for axis in ('x', 'y', 'z')))))

            # convert computed metrics (pos, velocity, acceleration, jerk) into desired position/vector formats
            for k, v in motion.iteritems():
                if k == 'p':
                    f = lambda k: to_lla(k, self.fixbuffer[0])
                else:
                    f = lambda k: to_vect(k, self.vector_mode)
                motion[k] = f(motion[k])

            motion['dt'] = dt
            return motion

def to_xyzt(fix, base):
    """map position fixes to a xyz-based coordinate system centered on 'base'
    we use the azimuthal equidistance projection-- the error should be
    indistinguishable on the scales we're working with
    """

    def p(f):
        return (f['lat'], f['lon'])
    def xy(mag, theta):
        return (mag * func(math.radians(theta)) for func in (math.sin, math.cos))

    t = u.fdelta(fix['time'] - base['time'])

    dist = geodesy.distance(p(base), p(fix))
    bearing = (geodesy.bearing(p(base), p(fix)) or 0.)
    rev_bearing = (geodesy.bearing(p(fix), p(base)) or 180.)

    x, y = xy(dist, bearing)
    z = fix['alt']

    if fix['speed'] is not None:
        heading = (fix['heading'] - rev_bearing) + (bearing + 180.)
        vx, vy = xy(fix['speed'], heading)
    else:
        vx = vy = None
    vz = fix['climb']

    return (t, (x, y, z), (vx, vy, vz))

def to_lla((x, y, z), base):
    """convert the xyz-based position back to lat-lon-alt"""
    dist = geodesy.vlen([x, y])
    bearing = geodesy._xy_to_bearing(x, y)
    ll = geodesy.plot((base['lat'], base['lon']), bearing, dist)[0]
    return (ll[0], ll[1], z)

def to_vect((x, y, z), mode):
    if mode == 'xyz':
        return (x, y, z)
    elif mode == 'raz':
        r = geodesy.vlen([x, y])
        return (r, geodesy._xy_to_bearing(x, y) if r > 0. else 0., z)
    elif mode == 'rai':
        r = geodesy.vlen([x, y, z])
        ll = geodesy.ecefu_to_ll(geodesy.vnorm([y, x, z])) if r > 0. else (0., 0.)
        return (r, ll[1], ll[0])

def split_by_axis(coords):
    """split transformed coordinates into groups by axis"""
    def to_xyz(c):
        t, p, v = c
        return [dict(zip(('t', 'p', 'v'), axis)) for axis in zip([t]*3, p, v)]

    by_axis = zip(*(to_xyz(c) for c in coords))
    return zip(('x', 'y', 'z'), by_axis)

def interpolate(axis, data, params={}):
    p = data[0]['p']
    if p is None:
        return None

    # 'none' amongst older points?

    experimental_mode = False

    def equations():
        for i, e in enumerate(data if experimental_mode else data[:1]):
            t = e['t']
            yield ((1, t, t**2, t**3), e['p'])
            if not experimental_mode:
                yield ((0, 1, 2*t, 3*t**2), e['v'] or 0.)

        yield((0,0,0,1),0.)
        yield((0,0,1,0),0.)
        yield((0,1,0,0),0.)

    factors = list(solve(*(np.array(m[:4]) for m in zip(*equations()))))
    return lambda dt: project(dt, factors)

# fix me
def polynomial_factors(degree, derivative=0):
    return [u.fact_div(i + derivative, i) for i in range(0, degree + 1)]

def project(t, factors):
    """evaluate a polynomial and its derivatives"""
    def polynomial(derivative):
        def weight(i):
            return u.fact_div(i + derivative, i)
        return sum(weight(i) * k * t**i for i, k in enumerate(factors[derivative:]))

    if factors:
        return [polynomial(i) for i in range(len(factors))]
    else:
        # return None for all derivatives -- we don't know how many, so use
        # infinite generator
        def _():
            while True:
                yield None
        return _()









def live_stream(gps_sub):
    """fix stream from a live gps"""
    while True:
        fix = gps_sub.get_fix()
        if fix:
            yield fix

def timeline_stream(stream):
    """stream wrapper that emits fixes at the designated timestamp"""
    for fix in stream:
        u.wait_until(u.to_timestamp(fix['time']))
        fix['systime'] = datetime.utcnow()
        yield fix

def dead_reckoning_stream(p0, v, interval=1.):
    """fix stream that simulates travel in a straight line"""
    def seq(t):
        while True:
            yield t
            t += interval

    t0 = time.time()
    def make_fix(t):
        p, bearing = geodesy.plot(p0, v[1], v[0] * (t - t0))
        return {'time': datetime.utcfromtimestamp(t), 'lat': p[0], 'lon': p[1], 'alt': None, 'speed': v[0], 'heading': bearing, 'climb': None}

    def fixstream():
        for t in seq(t0):
            yield make_fix(t)
    return timeline_stream(fixstream())

class TrackLogProvider(threading.Thread):
    """helper that buffers historical fixes from tracklog database and provides them
    to tracklog_stream"""

    # how much data to fetch in a single db query (in historical time)
    FETCH_WINDOW = timedelta(minutes=5)
    # how close to real time we're allowed to query
    PRESENT_THRESHOLD = timedelta(seconds=3)
    # minimum allowed query interval, to prevert too-frequent queries
    MIN_QUERY_INTERVAL = timedelta(seconds=0.5)

    def __init__(self, timeskew, buffer_window, dbsess, q):
        """
        timeskew -- function mapping real time to historical time
        buffer_window -- how many seconds' worth (in real time) of fixes
          to keep pre-buffered at all times
        dbsess -- db session
        q -- queue to provide fixes to tracklog_stream()
        """

        threading.Thread.__init__(self)
        self.daemon = True

        self.timeskew = timeskew
        self.buffer_window = timedelta(seconds=buffer_window)
        self.dbsess = dbsess
        self.q = q

        self.max_fetched = None

    def run(self):
        while True:
            # check if the latest buffered trackpoint covers us through 'buffer window' in real time
            if self.max_fetched is None or self.max_fetched < self.timeskew(datetime.utcnow() + self.buffer_window):
                present = datetime.utcnow() - self.PRESENT_THRESHOLD
                t_hist = self.timeskew(datetime.utcnow())
                start = self.max_fetched or t_hist
                end = min(start + self.FETCH_WINDOW, present)

                if t_hist >= present:
                    logging.info('reached the present; playback terminated')
                    break
                if end - start < self.MIN_QUERY_INTERVAL:
                    continue

                logging.debug('querying tracklog %s to %s' % (start, end))

                for f in query_tracklog(self.dbsess, start, end):
                    self.q.put(f)

                self.max_fetched = end
            time.sleep(0.01)

def tracklog_stream(dbconn, start, speedup=1., buffer_window=30.):
    """fix stream that plays back historical tracklogs

    dbconn -- connector to tracklog database
    start -- historical fix to begin streaming from (datetime)
    speedup -- multiplier for rate of playback vs. real-time
    buffer_window -- how much data to keep buffered (seconds)
    """

    t0 = datetime.utcnow()
    def real_to_hist_time(t):
        return start + timedelta(seconds=u.fdelta(t - t0) * speedup)
    def hist_to_real_time(t):
        return t0 + timedelta(seconds=u.fdelta(t - start) / speedup)

    q = Queue.Queue()
    dbsess = sessionmaker(bind=create_engine(settings.GPS_LOG_DB))()
    TrackLogProvider(real_to_hist_time, buffer_window, dbsess, q).start()

    def fix_fix(fix):
        fix['orig_time'] = fix['time']
        fix['time'] = hist_to_real_time(fix['time'])
        for vfield in ('speed', 'climb'):
            if fix[vfield]:
                fix[vfield] *= speedup
        return fix

    def fixstream():
        while True:
            yield fix_fix(q.get())
    return timeline_stream(fixstream())



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    fixstream = tracklog_stream('postgresql://geoloc', datetime.utcnow() - timedelta(seconds=31.), 1.)

    for f in fixstream:
        print f

