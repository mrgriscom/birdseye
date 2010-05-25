import socket
import threading
import Queue
import time
import pickle
import struct
from datetime import datetime
import sys
import logging

# this module listens directly to the gpsd output, aggregates
# and pre-processes the data, and dispatches it over a socket
# to other consumers

# it is intended to simplify the interface to gpsd, hiding
# some of its quirks/inaccuracies, especially for a particular
# device (BU-353)

GPSD_PORT = 2947
DISPATCH_PORT = 2948

#a socket wrapper that provides for sending/receiving entire
#messages, instead of operating on low-level buffers. has
#support for timeouts, but main use case is timing-out when no
#data has been sent/received. timeout mid-message on receive is
#supported because it's easy, but timeout mid-message on send
#is a fatal error. thus, this class is best used only for local
#socket communication
class linesocket:
  class BrokenConnection (Exception):
    pass

  class MidTransmissionTimeout (Exception):
    pass

  class CantConnect (Exception):
    pass

  def __init__ (self, sock=None):
    self.leftover = ''
    self.bufsize = 4096

    if sock == None:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.socket = sock

  def send (self, data):
    totalsent = 0
    try:
      while totalsent < len(data):
        sent = self.socket.send(data[totalsent:min(totalsent + self.bufsize, len(data))])
        if sent == 0:
          raise self.BrokenConnection()
        totalsent += sent
    except socket.timeout:
      if totalsent == 0:
        raise
      else:
        raise self.MidTransmissionTimeout()
    except socket.error:
      raise self.BrokenConnection()

  def readn (self, n):
    return self.readuntil(lambda data: len(data) >= n, lambda data: (n, 0))

  def readline (self):
    newline = '\r\n'
    return self.readuntil(lambda data: newline in data, lambda data: (data.find(newline), len(newline)))

  def readuntil (self, satisfaction, length):
    data = self.leftover
    try:
      while not satisfaction(data):
        data += self.readbuf()
    except socket.timeout:
      self.leftover = data
      raise

    (datalen, skip) = length(data)
    self.leftover = data[datalen + skip:]
    return data[0:datalen]

  def readbuf (self):
    try:
      fragment = self.socket.recv(self.bufsize)
      if len(fragment) == 0:
        raise self.BrokenConnection()
      return fragment
    except socket.timeout:
      raise
    except socket.error:
      raise self.BrokenConnection()

  def connect (self, host, port):
    try:
      self.socket.connect((host, port))
    except socket.error:
      raise self.CantConnect()

  def timeout (self, s):
    self.socket.settimeout(s)

  def close (self):
    self.socket.close()

#listener thread that connects to gpsd server, reads sentences,
#parses them in a basic manner and adds them to a message queue
class gps_listener (threading.Thread):
  def __init__  (self, q):
    threading.Thread.__init__(self)
    self.up = True

    self.last_data_at = None
    self.sat_info = None
    self.sirf_alert = False

    self.queue = q
    self.socket = linesocket()
    self.socket.timeout(3)
    try:
      self.socket.connect('localhost', GPSD_PORT)
    except linesocket.CantConnect:
      self.socket.close()
      raise

  def terminate (self):
    self.up = False

  def run (self):
    try:
      self.listen_gps()
    except:
      logging.exception('gpslistener')

  def listen_gps (self):
    self.socket.send('W=1')

    while self.up:
      try:
        line = self.socket.readline()
        self.last_data_at = time.time()

        data = self.parse_message(line)
        if data != None:
          (type, val) = data
          if type == 'nav':
            self.queue.put(val)
          elif type == 'sat':
            self.sat_info = val
      except socket.timeout:
        pass

    self.socket.close()
 
  def parse_message (self, message):
    nav_preamble = 'GPSD,O='
    sat_preamble = 'GPSD,Y='
    data = None

    if message.startswith(nav_preamble):
      pieces = message[len(nav_preamble):].split()
      if pieces[0] in ['GGA', 'GSA', 'RMC']:
        if len(pieces) >= 15:
          data = self.parse_nav_msg(pieces)
        else:
          logging.warn('message lacked expected data fields')
      else:
        if pieces[0] == '?':
          logging.debug('no fix data')
        else:
          logging.info('ignored message type [%s]' % pieces[0])
          if pieces[0].startswith('MID'):
            self.sirf_alert = True
    elif message.startswith(sat_preamble):
      pieces = message[len(sat_preamble):].split()
      if pieces[0] in ['GSV']:
        data = self.parse_sat_data(' '.join(pieces[2:]))
      else:
        logging.info('ignored message type [%s]' % pieces[0])
    else:
      logging.info('non-relevant message received')

    return data

  def parse_nav_msg (self, pieces):
    fix_types = {'?': None, '1': 'invalid', '2': '2d', '3': '3d'}

    data = {}
    data['msg_type'] = pieces[0]
    data['time']     = conv_float(pieces[1])
    data['lat']      = conv_float(pieces[3])
    data['lon']      = conv_float(pieces[4])
    data['alt']      = conv_float(pieces[5])
    data['h_error']  = conv_float(pieces[6])
    data['v_error']  = conv_float(pieces[7])
    data['speed']    = conv_float(pieces[9])
    data['heading']  = conv_float(pieces[8])
    data['climb']    = conv_float(pieces[10])
    data['fix_type'] = fix_types[pieces[14]]

    if data['time'] == None or data['time'] > time.time() + 1.0e7:
      #happens sometimes on the first sample after gpsd starts up
      logging.info('bad timestamp; ignoring report')
      return None
    
    return ('nav', data)

  def parse_sat_data (self, satinfo):
    try:
      pieces = satinfo.split(':')
      sats = {}
      for pc in pieces[1:]:
        if not pc:
          continue

        (id, alt, azi, snr, used) = [int(x) for x in pc.split()]
        sats[id] = {'alt': alt, 'azimuth': azi, 'snr': snr, 'used': (used == 1)}

      return ('sat', sats)
    except:
      return None




#aggregator/dispatcher thread that reads gpsd messages from a queue. it aggregates the data from
#multiple messages together to create a complete fix and then dispatches the fix when ready. if
#all the data for a single fix is taking too long to arrive, it will dispatch it prematurely (to
#remain timely) so long as minimally-required information is present. performs data-integrity and
#device-quirk cleanup on the fix data
class gps_dispatcher (threading.Thread):
  def __init__ (self, server):
    threading.Thread.__init__(self)
    self.up = True

    self.sample_window = 0.1 #s, how far apart the GPS timestamps can be before being considered separate samples
    self.buffer_window = 0.3 #s, how long we wait for all messages to come in before dispatching the sample

    self.server = server
    self.queue = Queue.Queue(-1)
    self.active_report = None    #timestamp of the sample we're currently collecting data for (or most recent sample)
    self.report_data = None      #aggregation buffer for current sample's data
    self.report_complete = False #whether the current sample has all necessary data and has been dispatched
    self.report_timeout = None   #timestamp of when to stop collecting data for current sample and dispatch it

    self.last_fix_at = None

  def terminate (self):
    self.up = False

  def run (self):
    try:
      while self.up:
        self.process_queue()
    except:
      logging.exception('gpsdispatcher')

  def process_queue (self):
    if (self.report_timeout == None):
      timeout = 1
      reportable_timeout = False
    else:
      timeout = max(self.report_timeout - time.time(), 0.)
      reportable_timeout = True

    try:
      data = self.queue.get(True, timeout)
      self.handle_data(data)
    except Queue.Empty:
      if reportable_timeout:
        logging.warn('timed out waiting for data')
        self.dispatch_report()

  def handle_data (self, data):
    if self.active_report == None:
      self.start_new_report(data)

    if abs(self.active_report - data['time']) < self.sample_window and not self.report_complete:
      self.aggregate_data(data)
      if self.is_report_complete():
        self.dispatch_report()
    elif data['time'] > self.active_report:
      if not self.report_complete:
        self.dispatch_report()
      self.start_new_report(data)
      self.handle_data(data)
    else:
      logging.warn('additional data received for sample already complete! type: %s, time: %f, current: %f' %
              (data['msg_type'], data['time'], self.active_report))

  def start_new_report (self, data):
    self.active_report = data['time']
    self.report_data = None
    self.report_complete = False
    self.report_timeout = time.time() + self.buffer_window

  def aggregate_data (self, data):
    #basic sanity checking for BU-353
    #expected_contents = {'GGA': ['time', 'lat', 'lon', 'alt', 'climb'],
    #                     'GSA': ['time', 'lat', 'lon', 'alt', 'h_error', 'v_error', 'climb', 'fix_type'],
    #                     'RMC': ['time', 'lat', 'lon', 'speed', 'heading', 'h_error', 'v_error', 'climb', 'fix_type']}
    #expected_fields = expected_contents[data['msg_type']]
    #for (key, value) in data.iteritems():
    #  if key != 'msg_type':
    #    if (value != None) != (key in expected_fields):
    #      log('BU-353: did not receive expected message contents; type: %s, field: %s, value: %s' %
    #             (data['msg_type'], key, str(value)))

    #real work
    if self.report_data == None:
      self.report_data = data
      self.report_data['msg_types'] = [self.report_data['msg_type']]
      del self.report_data['msg_type']
    else:
      for (key, value) in data.iteritems():
        if key == 'msg_type':
          self.report_data['msg_types'].append(value)
        elif key == 'time':
          pass
        elif value != None:
          if self.report_data[key] == None:
            self.report_data[key] = value
          elif self.report_data[key] != value:
            logging.warn('conflicting values among messages for same sample! [' + self.report_data[key] + ', ' + value + ']')
    
  def is_report_complete (self):
    if set(self.report_data['msg_types']) == set(['GGA', 'GSA', 'RMC']):
      return True
    else:
      for (key, value) in self.report_data.iteritems():
        if key != 'msg_types' and value == None:
          return False
      return True

  def dispatch_report (self):
    self.report_complete = True
    self.report_timeout = None
    report = self.report_data.copy()

    self.postprocess(report)
    self.dispatch(report)

  def postprocess (self, report):
    report['time'] = datetime.utcfromtimestamp(report['time'])
    del report['msg_types']
    if report['fix_type'] == None:
      report['fix_type'] = 'unknown'
    report['comment'] = None

    #cleanup for BU-353
    if float_eq(report['climb'], 0.):
      report['climb'] = None
      addcomment(report, 'ign-zero-climb')
    else:
      logging.info('BU-353: detected report with non-zero climb!')

    if float_eq(report['v_error'], 8.0):
      report['v_error'] = None
      addcomment(report, 'ign-perfect-vdop')
      logging.info('BU-353: removing untrustworthy v-error')

    #further cleanup of error estimates?

    #data integrity
    if report['fix_type'] == '2d' and report['alt'] != None:
      report['alt'] = None
      addcomment(report, 'drop-alt-2d')
      logging.debug('removing altitude on 2-d fix')

    if report['alt'] == None:
      if report['climb'] != None:
        report['climb'] = None
        addcomment(report, 'drop-climb-noalt')
        logging.debug('removing climb w/o altitude')

      if report['v_error'] != None:
        report['v_error'] = None
        addcomment(report, 'drop-vdop-noalt')
        logging.debug('removing err_v w/o altitude')

  def dispatch (self, report):
    if self.report_sufficient(report):
      self.server.broadcast(report)
      self.last_fix_at = time.time()
    else:
      logging.warn('report does not contain minimally-required data')   

  def report_sufficient (self, report):
    for key in ['time', 'lat', 'lon']:
      if report[key] == None:
        return False
    return True

#creates a server socket, manages listeners, and broadcasts ready-for-consumption fixes
#produced by the dispatcher
class gps_server (threading.Thread):
  def __init__ (self):
    threading.Thread.__init__(self)
    self.up = True

    self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.serversocket.settimeout(1)
    try:
      self.serversocket.bind(('localhost', DISPATCH_PORT))
      self.serversocket.listen(5)
    except socket.error:
      self.serversocket.close()
      raise

    self.clientlist = []
    self.listlock = threading.Lock()
  
  def terminate (self):
    self.up = False

  def run (self):
    try:
      while self.up:
        try:
          (clientsocket, address) = self.serversocket.accept()
          self.addclient(clientsocket)
        except socket.timeout:
          pass

      self.close()
    except:
      logging.exception('gpsserver')

  def addclient (self, socket):
    lsocket = linesocket(socket)
    lsocket.timeout(3)

    self.listlock.acquire()
    self.clientlist.append(lsocket)
    logging.info('subscriber added. %d total' % len(self.clientlist))
    self.listlock.release()

  def broadcast (self, data):
    pdata = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
    message = struct.pack('i', len(pdata)) + pdata

    deadconnections = []
    self.listlock.acquire()
    for client in self.clientlist:
      try:
        client.send(message)
      except (linesocket.BrokenConnection, socket.timeout, linesocket.MidTransmissionTimeout):
        deadconnections.append(client)

    for dead in deadconnections:
      self.clientlist.remove(dead)
      dead.close()

    if len(deadconnections) > 0:
      logging.info('%d subscriber(s) lost. %d remaining' % (len(deadconnections), len(self.clientlist)))
    self.listlock.release()

  def close (self):
    self.serversocket.close()
    self.listlock.acquire()
    for client in self.clientlist:
      client.close()
    self.clientlist = []
    self.listlock.release()
    
def float_eq (a, b):
  if a == None or b == None:
    return a == b
  else:
    return abs(a - b) < 1.0e-9

def conv_float (str):
  if str == '?':
    return None
  else:
    return float(str)

def addcomment (report, comment):
  if report['comment'] == None:
    report['comment'] = comment
  else:
    report['comment'] += ';' + comment

#subscriber access
class gps_subscription ():
  def __init__ (self, timeout=3):
    self.sock = linesocket()
    self.sock.timeout(timeout)
    try:
      self.sock.connect('localhost', DISPATCH_PORT)
    except linesocket.CantConnect:
      self.sock.close()
      raise

  def get_fix (self):
    try:
      len = struct.unpack('i', self.sock.readn(4))[0]
      data = pickle.loads(self.sock.readn(len))
    except socket.timeout:
      return None

    data['systime'] = datetime.utcnow()
    return data

  def unsubscribe (self):
    self.sock.close()

#main thread that starts up all threads, monitors their health, and shuts them all down
#if a problem is detected
if __name__ == '__main__':

  def all_alive (threads):
    return all([t.isAlive() for t in threads])

  def all_dead (threads):
    return not any([t.isAlive() for t in threads])

  try:
    server = gps_server()
  except socket.error:
    logging.error('cannot bind to dispatcher port %s' % DISPATCH_PORT)
    sys.exit()

  dispatcher = gps_dispatcher(server)

  try:
    listener = gps_listener(dispatcher.queue)
  except linesocket.CantConnect:
    logging.error('cannot connect to gpsd service')
    sys.exit()

  threads = [server, listener, dispatcher]
  for t in threads:
    t.start()

  running = True
  try:
    while running:
      time.sleep(1)
      if not all_alive(threads):
        logging.error('thread encountered fatal error')
        running = False
  except KeyboardInterrupt:
    logging.info('shutdown request from user')

  logging.info('shutting down...')
  for t in reversed(threads):
    t.terminate()
  
  while not all_dead(threads):
    time.sleep(1)
  logging.info('shut down complete')
  sys.exit()
