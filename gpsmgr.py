import threading
import curses
import time
import os
import subprocess
import signal
import re
from gps import gpslistener
from gps import gpslogger
from util.messaging import MessageSocket
import util.util as u
import math
import logging
import zmq
import settings
from optparse import OptionParser
import collections

class Monitor(threading.Thread):
    """a thread that monitors some other activity and maintains a
    queryable status"""

    def __init__(self, poll_interval):
        threading.Thread.__init__(self)
        self.poll_interval = poll_interval
        self.up = True
        self.lock = threading.Lock()
        self.raw_status = None
  
    def terminate(self):
        self.up = False
        self.cleanup()

    def run(self):
        while self.up:
            self.poll_status()
            time.sleep(self.poll_interval)

    def cleanup(self):
        pass

    def poll_status(self):
        with self.lock:
            self.raw_status = self.get_status()

    def get_status(self):
        """override: return data representing the current status"""
        raise Exception('override me')

    def status(self):
        with self.lock:
            return self.format_status(self.raw_status) if self.raw_status is not None else None

    def format_status(self, raw):
        """override: given result from get_status(), format into
          a human-readable status message"""
        return str(raw)

class DeviceWatcher(Monitor):
    """monitor if gps device is present at hardware level"""

    def __init__(self, device_name, poll_interval=0.2):
        Monitor.__init__(self, poll_interval)
        self.device_name = device_name

    def get_status(self):
        return os.path.exists(self.device_name)

    def format_status(self, connected):
        return 'device connected' if connected else 'device not connected'

class GPSDProcess(threading.Thread):
    """wrapper around the gpsd system process"""

    def __init__(self, device, rate):
        threading.Thread.__init__(self)
        self.device = device
        self.rate = rate
        self.status = {}
        self.statuslock = threading.Lock()

    def boot(self):
        self.set_rate()
        command = 'gpsd -b -N -n -D 2 %s' % self.device
        self.p = subprocess.Popen(command.split(), stderr=subprocess.PIPE)

    def run(self):
        self.boot()

        while True:
            line = self.p.stderr.readline()
            if line:
                logging.debug(line.strip())
                self.process_output(line.strip())
            else:
                break

    def terminate(self):
        os.kill(self.p.pid, signal.SIGTERM)

    #this isn't a good idea; it tends to crash gpsd and/or make it behave strangely
    def flash(self):
        self.set_rate()
        os.kill(self.p.pid, signal.SIGHUP)

    def process_output(self, ln):
        with self.statuslock:
            if 'device open failed' in ln:
                self.status['online'] = False
            elif 'GPS is offline' in ln:
                self.status['online'] = False
            elif any(k in ln for k in ('opened GPS', 'activated GPS')):
                self.status['online'] = True
            elif 'already running' in ln:
                self.status['rogue'] = True
            else:
                m = re.search('speed +([0-9]+)', ln)
                if m:
                    self.status['speed'] = int(m.group(1))

    def get_status(self):
        with self.statuslock:
            return dict(self.status)

    def set_rate(self, baud=None):
        command = 'stty -F %s %d' % (self.device, baud or self.rate)
        subprocess.Popen(command.split(), stderr=subprocess.PIPE)

class GPSD(Monitor):
    """monitor the status of the gpsd daemon"""

    def __init__(self, device, rate, poll_interval=0.2):
        Monitor.__init__(self, poll_interval)
        self.gpsd_args = (device, rate)
        self.gpsd = None

    def get_status(self):
        status = self.gpsd.get_status() if self.gpsd else {}
        status['up'] = (self.gpsd and self.gpsd.isAlive())
        return status

    def format_status(self, status):
        if status['up']:
            online = status.get('online')
            speed = status.get('speed')

            msg = {
                True: 'gpsd online',
                False: 'gpsd up; device offline',
                None: 'gpsd up; searching for device...',
            }[online]

            if online and speed != self.gpsd.rate:
                msg += '; ' + ('slow! (%.1f)' % (speed / 1000.) if speed else 'speed unknown')

            return msg
        else:
            return 'gpsd not up' + (' (rogue instance?)' if status.get('rogue') else '')

    def start_gpsd(self):
        if self.gpsd == None or not self.gpsd.isAlive():
            self.gpsd = GPSDProcess(*self.gpsd_args)
            self.gpsd.start()
        else:
            logging.warn('gpsd already running')

    def stop_gpsd(self):
        if self.gpsd != None and self.gpsd.isAlive():
            self.gpsd.terminate()
            try:
                u.wait(10., lambda: not self.gpsd.isAlive())
                if self.gpsd.isAlive():
                    logging.error('gpsd didn\'t terminate after lengthy wait!')
            except u.Interrupted:
                pass
        else:
            logging.warn('gpsd not running')

    def restart_gpsd(self):
        self.stop_gpsd()
        self.start_gpsd()

    #this isn't a good idea; it tends to crash gpsd and/or make it behave strangely
    def flash_gpsd(self):
        self.gpsd.flash()

    def cleanup(self):
        self.stop_gpsd()

class DispatchLoader(threading.Thread):
    """initialize the gps broadcaster (intermediary between gpsd and our clients), and
    keep it up against all odds.

    we assume our own sockets are reliable, and only initialize them once. however, the
    socket that connects to gpsd is re-initialized as needed.
    """

    SERVER_RETRY_WAIT = 5.
    LISTENER_RETRY_WAIT = 3.

    def __init__(self, device_policy):
        threading.Thread.__init__(self)
        
        self.up = True
        self.device_policy = device_policy

        self.server_acquire = None
        self.server = None

        self.dispatcher = None

        self.listener_acquire = None
        self.listener = None
        self.listener_first_conn = False

    def terminate(self):
        self.up = False

    def run(self):
        self.server_acquire = u.Acquirer(gpslistener.GPSServer, self.SERVER_RETRY_WAIT, zmq.ZMQError)
        self.server = self.server_acquire.acquire(lambda: not self.up)

        self.dispatcher = gpslistener.GPSDispatcher(*filter(lambda e: e, [self.server, self.device_policy]))
        self.dispatcher.start()

        def mk_listener_acquire():
            return u.Acquirer(lambda: gpslistener.GPSListener(self.dispatcher.queue), self.LISTENER_RETRY_WAIT, MessageSocket.ConnectionFailed)

        time.sleep(0.5)
        self.listener_acquire = mk_listener_acquire()
        try:
            while self.up:
                self.listener = self.listener_acquire.acquire(lambda: not self.up, not self.listener_first_conn)
                self.listener.start()
                self.listener_first_conn = True

                while self.up:
                    time.sleep(0.01)
                    if not self.listener.isAlive():
                        self.listener = None
                        self.listener_acquire = mk_listener_acquire()
                        # note: clearing 'listener' before calling acquire() gives a (very) small
                        # window for the status message to revert to generic instead of 'retry in
                        # X'... don't really care
                        break

            if self.listener:
                self.listener.terminate()
        finally:
            self.dispatcher.terminate()
            self.server.close()

class DispatchWatcher(Monitor):
    def __init__(self, device_policy=None, poll_interval=0.2):
        Monitor.__init__(self, poll_interval)
        self.loader_args = (device_policy,)
        self.loader = None

    def get_status(self):
        l = self.loader
        if l == None:
            return 'listener not up'
        elif l.server == None:
            if retry_at(l.server_acquire) == None:
                return 'loading dispatcher...'
            else:
                return 'dispatcher: can\'t bind port; retry in %d' % retry_in(l.server_acquire)
        elif l.listener == None:
            if retry_at(l.listener_acquire) == None:
                return 'loading listener...'
            else:
                return '%s; retry in %d' % ('gpsd terminated' if l.listener_first_conn else 'can\'t connect to gpsd', retry_in(l.listener_acquire))
        else:
            since_fix = time.time() - l.dispatcher.last_fix_at if l.dispatcher.last_fix_at else None
            since_ping = time.time() - l.listener.last_data_at if l.listener.last_data_at else None

            if since_ping == None:
                return 'no data yet from gpsd'
            elif since_ping > 10.:
                return 'no data from gpsd in %d' % int(since_ping)
            elif l.listener.sirf_alert:
                return 'gps is in SiRF mode!'
            elif since_fix != None and since_fix < 5.:
                return 'lookin\' good!'
            else:
                msg = ('no fix in %d' % int(since_fix)) if since_fix != None else 'no fix yet'
                msg += '; ' + (sat_status(l.listener.sat_info) if l.listener.sat_info else 'no sat info')
                return msg

    def load(self):
        self.loader = DispatchLoader(*self.loader_args)
        self.loader.start()

    def cleanup(self):
        if self.loader:
            self.loader.terminate()

def sat_status(satinfo):
    def bucket(snr):
        categories = [(40., 'vg'), (30., 'g'), (20., 'b'), (0., 'vb')]
        for thresh, label in categories:
            if snr >= thresh:
                return label
    buckets = u.map_reduce(satinfo.values(), lambda s: [(bucket(s['snr']),)], len)
    buckets['ttl'] = len(satinfo)
    return '%(ttl)d sats %(vg)dvg/%(g)dg/%(b)db/%(vb)dvb' % collections.defaultdict(lambda: 0, buckets)

class LogWatcher(Monitor):
    """monitor the tracklogger

    note: we only initialize the logger/acquire the subscription once, and assume
    it is persistent and reliable (reasonable assumption, because both ends of that
    socket are under our control)"""

    def __init__(self, tracklog_db, poll_interval=1.):
        Monitor.__init__(self, poll_interval)
        self.tracklog_db = tracklog_db
        self.logger = None

    def launch(self):
        self.logger = gpslogger.GPSLogger(self.tracklog_db)
        self.logger.start()

    def cleanup(self):
        if self.logger:
            self.logger.terminate()

    def get_status(self):
        if self.logger:
            if not self.logger.dbsess:
                return 'not connected to db'
            elif not self.logger.gps:
                if retry_at(self.logger.gps_acquire) != None:
                    return 'can\'t connect to gps; retry in %d' % retry_in(self.logger.gps_acquire)
                else:
                    return 'connecting to gps...'
            else:
                return 'up; %d fixes buffered' % self.logger.buffer_size()
        else:
            return 'logger down'


def retry_at(acq):
    return acq.retry_at if acq else None

def retry_in(acq):
    return int(math.ceil(acq.retry_at - time.time()))


def sighup(type, frame):
    global HUP_recvd
    HUP_recvd = True

HUP_recvd = False
def init_signal_handlers():
    signal.signal(signal.SIGHUP, sighup)




def loader(options):
    curses.wrapper(loader_curses, options)

def loader_curses(win, options):
    curses.curs_set(0)
    win.nodelay(1)

    w_device = DeviceWatcher(settings.GPS_DEVICE)
    w_device.start()

    w_gpsd = GPSD(settings.GPS_DEVICE, settings.BAUD_RATE)
    w_gpsd.start()
    w_gpsd.start_gpsd()

    w_dispatch = DispatchWatcher(u.try_import(settings.GPS_DEVICE_POLICY)() if settings.GPS_DEVICE_POLICY else None)
    w_dispatch.start()
    w_dispatch.load()

    w_tracklog = LogWatcher(settings.GPS_LOG_DB)
    w_tracklog.start()
    if options.tracklog:
        w_tracklog.launch()

    watchers = [
        (w_device, 'GPS Device'),
        (w_gpsd, 'GPS Daemon'),
        (w_dispatch, 'GPS Dispatcher'),
        (w_tracklog, 'GPS Logger'),
    ]

    def restart():
        w_gpsd.restart_gpsd()

    #def flash():
    #    w_gpsd.flash_gpsd()
    #    # TODO: trigger listener to reconnect

    try:
        while True:
            for i, (watcher, caption) in enumerate(watchers):
                println(win, fmt_line(caption, watcher.status()), i + 1, 2)

            println(win, 'ESC:   shut down', 8, 2)
            println(win, 'F2:    reconnect gps', 9, 2)

            try:
                key = win.getkey()
            except:
                key = ''

            if key in ('\x1b', '^[', 'q'):
                break
            elif key in ('KEY_F(2)', ' '):
                restart()
            #elif key == 'KEY_F(3)':
            #    flash()

            global HUP_recvd
            if HUP_recvd:
                logging.debug('HUP!')
                HUP_recvd = False
                restart()

            time.sleep(0.01)
    except KeyboardInterrupt:
        pass

    w_device.terminate()
    w_gpsd.terminate()
    w_dispatch.terminate()
    w_tracklog.terminate()

def fmt_line(header, status):
    HEADER_WIDTH = 20
    STATUS_WIDTH = 45

    if status == None:
        status = '-- no status --'

    return header.ljust(HEADER_WIDTH) + ' [' + status.rjust(STATUS_WIDTH) + ']'

def println(w, str, y, x=0):
    w.addstr(y, x, str)
    w.clrtoeol()
    w.refresh()


if __name__ == "__main__":
    settings.init_logging()
    init_signal_handlers()

    parser = OptionParser()
    parser.add_option('-x', '--no-tracklog', dest='tracklog', action='store_false', default=True,
                      help='don\'t store tracklog')

    (options, args) = parser.parse_args()

    loader(options)

  
