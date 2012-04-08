import threading
import os
import time
from util import geodesy
from gps import gpslistener
from gps.gpslogger import Fix
import sys
from datetime import datetime, timedelta
import util.util as u
import Queue
import settings
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Tracker(threading.Thread):
    def __init__(self, fixstream):
        threading.Thread.__init__(self)
        self.daemon = True
        self.lock = threading.Lock()

        self.fixstream = fixstream

        self.p = None
        self.v = None
        self.t = None

    def run (self):
        for fix in self.fixstream:
            p = (data['lat'], data['lon'], data['alt'])
            v = (data['speed'], data['heading'], data['climb'])
            self.update_loc

        while self.up:
            fix = self.fixstream.next()
            self.update_loc(fix)

    def update_loc(self):
        with self.lock:
            self.p = p
            self.v = v
            self.t = time.time()

    def get_loc(self):
        with self.lock:
            if not self.p:
                return None
            else:
                dt = time.time() - self.t
                p2 = geodesy.plot(self.p, self.v[1] or 0., (self.v[0] or 0.) * dt)

                if self.p[3] is not None:
                    alt = self.p[3] + dt * (v[3] or 0.)
                else:
                    alt = None

                p = (p2[0], p2[1], alt)

                # v needs to be adjusted for curvature, technically
                return (p, self.v, dt)

# assume all received fixes are received in real-time??

    """
    bearing = geodesy.bearing(p2, self.p)
      if bearing != None:
        if abs(geodesy.anglenorm(geodesy.bearing(self.p, p)) - v[1]) < 1.0e-3:
          bearing += 180.
          v = (v[0], geodesy.anglenorm(bearing))
    """

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

def demo_stream(p0, v, interval=1.):
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
    """helper that reads historical fixes from tracklog database and provides them
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

                fixes = [f.unpack() for f in self.dbsess.query(Fix).filter(Fix.gps_time >= start).filter(Fix.gps_time < end)]
                for f in sorted(fixes, key=lambda f: f['time']):
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

    fixstream = tracklog_stream('postgresql://geoloc', datetime.utcnow() - timedelta(seconds=40.), 1.)

    for f in fixstream:
        print f

