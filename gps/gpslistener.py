import socket
import threading
import Queue
import time
from datetime import datetime
import sys
import logging
from util.messaging import MessageSocket
import util.util as u
import zmq
import json
import pickle
import settings
from contextlib import contextmanager
import math

# this module listens directly to the gpsd output, aggregates
# and pre-processes the data, and dispatches it over a socket
# to other consumers

# it is intended to simplify the interface to gpsd, hiding
# some of its quirks/inaccuracies, especially for particular
# devices

GPSD_PORT = 2947
DISPATCH_PORT = GPSD_PORT + 1

class GPSListener(threading.Thread):
    """listener thread that connects to gpsd server, reads sentences,
    parses them in a basic manner and adds them to a message queue
    """

    def __init__(self, q):
        threading.Thread.__init__(self)
        self.up = True

        self.last_data_at = None
        self.sat_info = None

        # i don't like sirf mode -- deal with raw nmea
        # mostly because my gps puck has a soothing flashing led when it has
        # a lock in nmea mode, but not so in sirf mode
        self.sirf_alert = False

        self.queue = q
        self.socket = MessageSocket()
        try:
            self.socket.connect(GPSD_PORT, conn_timeout=3.)
        except MessageSocket.ConnectionFailed:
            self.socket.close()
            raise

    def terminate(self):
        self.up = False

    def run(self):
        try:
            self.listen_gps()
        except MessageSocket.ConnectionBroken:
            logging.warn('gpslistener: broken connection; exiting...')
        except:
            logging.exception('gpslistener')

    def listen_gps(self):
        # enable watcher mode
        self.socket.send('W=1\n') # old protocol
        self.socket.send('?WATCH={"class":"WATCH","json":true}\n') # new protocol

        while self.up:
            try:
                line = self.socket.readline()
                self.last_data_at = time.time()

                data = self.parse_message(line)
                if data:
                    (type, val) = data
                    if type == 'nav':
                        self.queue.put(val)
                    elif type == 'sat':
                        self.sat_info = val
            except socket.timeout:
                pass

        self.socket.close()

    def parse_message(self, message):
        try:
            message = json.loads(message)
        except ValueError:
            # json parse error; parse as old protocol
            message = parse_message_old(message)
        if message is None:
            return None

        if message.get('tag', '').startswith('MID'):
            self.sirf_alert = True

        if message['class'] == 'TPV':
            return process_tpv(message)
        elif message['class'] == 'SKY':
            return process_sat(message)
        else:
            logging.debug('ignoring message of class [%s]' % message['class'])
            return None

def process_tpv(message):
    fields = {
        'tag': 'tag',
        'time': 'time',
        'lat': 'lat',
        'lon': 'lon',
        'alt': 'alt',
        'h_error': 'eph',
        'v_error': 'epv',
        'speed': 'speed',
        'heading': 'track',
        'climb': 'climb',
    }

    data = dict((k, message.get(v)) for k, v in fields.iteritems())
    if not data['h_error']:
        epx = message.get('epx')
        epy = message.get('epy')
        if epx and epy:
            data['h_error'] = math.sqrt(epx * epy)
        else:
            data['h_error'] = epx or epy
    data['fix_type'] = {1: 'invalid', 2: '2d', 3: '3d'}.get(message.get('mode'))
    
    if data['fix_type'] == 'invalid':
        return None
    
    if data['time'] == None or data['time'] > time.time() + 1.0e7:
        # happens sometimes on the first sample after gpsd starts up
        logging.info('bad timestamp; ignoring report')
        return None

    return ('nav', data)

def process_sat(message):
    fields = {
        'elev': 'el',
        'azimuth': 'az',
        'snr': 'ss',
        'used': 'used',
    }

    satinfo = message.get('satellites')
    if not satinfo:
        return None

    return ('sat', dict((s['PRN'], dict((k, s[v]) for k, v in fields.iteritems()))for s in satinfo))

def parse_message_old(message):
    handlers = {
        'GPSD,O=': parse_nav_msg,
        'GPSD,Y=': parse_sat_msg,
    }

    for preamble, handler in handlers.iteritems():
        if message.startswith(preamble): 
            pieces = message[len(preamble):].split()
            try:
                return handler(pieces)
            except:
                logging.exception('error parsing gpsd message')
    # non-relevant message class
    return None

def parse_nav_msg(pieces):
    tag = pieces[0]
    if tag in ['GGA', 'RMC', 'GLL', 'GSA', 'MID2']: # GSA is weird, but gpsd outputs it sometimes, maybe for error fields?
        def conv_float(s):
            return None if s == '?' else float(s)

        data = {
            'class': 'TPV',
            'tag': tag,
            'time': conv_float(pieces[1]),
            'lat': conv_float(pieces[3]),
            'lon': conv_float(pieces[4]),
            'alt': conv_float(pieces[5]),
            'eph': conv_float(pieces[6]),
            'epv': conv_float(pieces[7]),
            'speed': conv_float(pieces[9]),
            'track': conv_float(pieces[8]),
            'climb': conv_float(pieces[10]),
        }
        try:
            data['mode'] = int(pieces[14])
        except ValueError:
            pass
        
        return data
    elif tag == '?':
        # no fix data
        return {'class': 'TPV', 'mode': 1}
    else:
        logging.info('ignored message type [%s]' % tag)
        return None

def parse_sat_msg(pieces):
    tag = pieces[0]
    if tag in ['GSV', 'MID4']:
        satinfo = ' '.join(pieces[2:]).split(':')

        def make_sat(raw):
            s = dict(zip(('PRN', 'el', 'az', 'ss', 'used'), (int(x) for x in raw.split())))
            s['used'] = bool(s['used'])
            return s

        return {'class': 'SKY', 'tag': tag, 'satellites': [make_sat(s) for s in satinfo[1:] if s]}
    elif tag in ['GSA']:
        # ignore
        return None
    else:
        logging.info('ignored message type [%s]' % tag)
        return None


class StubDevicePolicy(object):
    def expected_fields(self, ef):
        return ef

    def sufficient_tags(self):
        return None

    def cleanup(self, report):
        return report



class GPSDispatcher(threading.Thread):
    """aggregator/dispatcher thread that reads gpsd messages from a queue. it aggregates
    the data from multiple messages together to create a complete fix and then dispatches
    the fix when ready. if all the data for a single fix is taking too long to arrive, it
    will dispatch it prematurely (to remain timely) so long as minimally-required
    information is present. performs data-integrity and device-quirk cleanup on the fix
    data
    """

    def __init__(self, server, device_policy=StubDevicePolicy()):
        threading.Thread.__init__(self)
        self.up = True

        self.server = server
        self.device_policy = device_policy
        self.queue = Queue.Queue()

        self.active_report = None    # aggregation buffer for current sample's data
        self.report_timeout = None   # timestamp of when to stop collecting data for current sample and dispatch it

        self.last_fix_at = None

    def terminate(self):
        self.up = False

    def run(self):
        try:
            while self.up:
                self.process_queue()
        except:
            logging.exception('gpsdispatcher')

    def process_queue(self):
        def dispatch_by_timeout():
            logging.warn('timed out waiting for data')
            self.dispatch_report()

        if self.report_timeout is None:
            timeout = 1
            ontimeout = lambda: None
        else:
            timeout = max(self.report_timeout - time.time(), 0.)
            ontimeout = dispatch_by_timeout

        try:
            data = self.queue.get(True, timeout)
            self.handle_data(data)
        except Queue.Empty:
            ontimeout()

    def handle_data(self, data):
        if self.active_report == None:
            self.start_new_report(data)

        status = self.sample_timeline(data)
        if status == 'same-sample' and not self.is_dispatched():
            self.aggregate_data(data)
            if self.is_report_complete():
                self.dispatch_report()
        elif status == 'new-sample':
            if not self.is_dispatched():
                self.dispatch_report()
            self.start_new_report(data)
            self.handle_data(data)
        else:
            self.superfluous_sample(data)

    def start_new_report(self, data):
        self.active_report = {'time': data['time'], 'tags': set()}
        self.report_timeout = time.time() + settings.GPS_BUFFER_WINDOW

    def sample_timeline(self, data):
        if abs(self.active_report['time'] - data['time']) < settings.GPS_SAMPLE_WINDOW:
            return 'same-sample'
        elif data['time'] > self.active_report['time']:
            return 'new-sample'
        else:
            return 'old-sample'

    def aggregate_data(self, data):
        self.active_report['tags'].add(data['tag'])
        for field, new_val in data.iteritems():
            if field in ('tag', 'time'):
                continue
            if new_val is None:
                continue
            
            old_val = self.active_report.get(field)
            if old_val is not None and new_val != old_val:
                logging.warn('conflicting values among messages for same sample! [%s, %s]' % (old_val, new_val))
                continue
            self.active_report[field] = new_val

    def report_sufficient(self, report, fields=['time', 'lat', 'lon']):
        """whether report contains minimum-required nav data to be useful"""
        return all(report.get(k) for k in fields)
        
    def is_report_complete(self):
        """whether report contains all the data we want"""
        expected_fields = ['time', 'lat', 'lon', 'speed', 'heading', 'h_error']
        if self.active_report.get('fix_type') != '2d':
            expected_fields.extend(['alt', 'v_error'])
        expected_fields = self.device_policy.expected_fields(expected_fields)

        sufficient_tags = self.device_policy.sufficient_tags()

        if sufficient_tags and set(sufficient_tags) <= self.active_report['tags']:
            return True
        else:
            return self.report_sufficient(self.active_report, expected_fields)

    def superfluous_sample(self, data):
        logging.warn('additional data received for sample already dispatched: type: %s, time: %f, current: %f' %
                     (data['tag'], data['time'], self.active_report['time']))

    def is_dispatched(self):
        return self.active_report.get('dispatched', False)

    def dispatch_report(self):
        assert not self.is_dispatched()

        self.active_report['dispatched'] = True
        self.report_timeout = None

        self.dispatch(self.postprocess())

    def postprocess(self):
        report_fields = ['time', 'lat', 'lon', 'alt', 'h_error', 'v_error', 'speed', 'heading', 'climb', 'fix_type']
        report = dict((k, self.active_report.get(k)) for k in report_fields)

        report['time'] = datetime.utcfromtimestamp(report['time'])
        report['comments'] = []
        def addcomment(comment):
            report['comments'].append(comment)

        def clear_field(field, comment):
            if report[field] is not None:
                report[field] = None
                addcomment(comment)

        if report['fix_type'] == '2d':
            clear_field('alt', 'drop-alt-2d')

        if report['alt'] is None:
            clear_field('climb', 'drop-climb-noalt')
            clear_field('v_error', 'drop-vdop-noalt')

        for fe in ('h_error', 'v_error'):
            if u.f_eq(report[fe], 0.):
                report[fe] = None

        if report['speed'] is None:
            clear_field('heading', 'v-wo-bear')
        if report['heading'] is None:
            if u.f_eq(report['speed'], 0.):
                report['heading'] = 0.
            else:
                clear_field('speed', 'bear-wo-v')

        if report['speed'] is not None:
            if report['heading'] < 0:
                report['heading'] += 360.

            if report['speed'] < 0.:
                report['speed'] = -report['speed']
                report['heading'] = (report['heading'] + 180.) % 360.

        self.device_policy.cleanup(report)
        return report

    def dispatch(self, report):
        if self.report_sufficient(report):
            self.server.broadcast(report)
            self.last_fix_at = time.time()
        else:
            logging.warn('report does not contain minimally-required data')     

class GPSServer(object):
    """socket to broadcast out position fixes"""

    def __init__(self):
        self.context = zmq.Context()
        self.sock = self.context.socket(zmq.PUB)
        self.sock.bind('tcp://*:%d' % DISPATCH_PORT)

    def broadcast(self, data):
        self.sock.send(pickle.dumps(data))

    def close(self):
        self.sock.close()
        self.context.term()

class GPSSubscription(object):
    def __init__(self):
        # support for a connection timeout?
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect("tcp://localhost:%d" % DISPATCH_PORT)
        self.socket.setsockopt(zmq.SUBSCRIBE, '')

        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def get_fix(self, timeout=1.):
        result = dict(self.poller.poll(1000 * timeout))
        if result.get(self.socket) == zmq.POLLIN:
            data = pickle.loads(self.socket.recv())
            data['systime'] = datetime.utcnow()
            return data
        else:
            return None

    def unsubscribe(self):
        self.socket.close()
        self.context.term()

class GPSSubscriber(u.Acquirer):
    def __init__(self, retry_interval):
        u.Acquirer.__init__(self, GPSSubscription, retry_interval, zmq.ZMQError)

class BU353DevicePolicy(StubDevicePolicy):
    def cleanup(self, report):
        def clean(field, predicate, comment):
            if predicate(report[field]):
                report[field] = None
                report['comments'].append(comment)

        clean('climb', lambda x: u.f_eq(x, 0.), 'ign-zero-climb')
        clean('v_error', lambda x: u.f_eq(x, 8.), 'ign-perfect-vdop')

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    try:
        server = GPSServer()
        dispatcher = GPSDispatcher(server, BU353DevicePolicy())
    except zmq.ZMQError:
        logging.exception('cannot bind dispatcher')
        sys.exit()

    try:
        listener = GPSListener(dispatcher.queue)
    except MessageSocket.ConnectionFailed:
        logging.error('cannot connect to gpsd service')
        sys.exit()

    threads = [listener, dispatcher]
    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(0.1)
            if not all(t.is_alive() for t in threads):
                logging.error('thread encountered fatal error')
                break
    except KeyboardInterrupt:
        logging.info('shutdown request from user')

    logging.info('shutting down...')
    for t in reversed(threads):
        t.terminate()
    for t in threads:
        t.join()
    server.close()

    logging.info('shut down complete')
    sys.exit()
