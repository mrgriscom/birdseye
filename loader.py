import threading
import curses
import time
import os
import sys
import subprocess
import signal
import Queue
import re
from gps import gpslistener
import socket
import math
import logging
import logging.handlers

DEVICE = '/dev/ttyUSB0'
BAUD = 57600
POLL_INTERVAL = 1.

def init_logging ():
  LOG_FILE = 'birdseye.log'
  root = logging.getLogger()
  root.setLevel(logging.DEBUG)
  handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=2**20, backupCount=3)
  handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
  root.addHandler(handler)
init_logging()

class monitor_thread (threading.Thread):
  def __init__ (self, poll_interval=None):
    threading.Thread.__init__(self)
    self.poll_interval = poll_interval if poll_interval != None else POLL_INTERVAL
    self.up = True
    self.lock = threading.Lock()
    self.status_data = None
  
  def terminate (self):
    self.up = False
    self.cleanup()

  def run (self):
    while self.up:
      self.body()
      time.sleep(self.poll_interval)

  def body (self):
    self.set()

  def cleanup (self):
    pass

  def set (self):
    self.lock.acquire()
    self.status_data = self.get_status(self.status_data)
    self.lock.release()

  def get_status (self, current_status):
    raise Exception('abstract method')

  def status (self):
    self.lock.acquire()
    info = self.get_status_info(self.status_data) if self.status_data != None else None
    self.lock.release()
    return info

  def get_status_info (self, status_data):
    raise Exception('abstract method')

class device_watcher (monitor_thread):
  def __init__ (self, device_name, poll_interval=None):
    monitor_thread.__init__(self, poll_interval)
    self.device_name = device_name

  def get_status (self, _):
    return os.path.exists(self.device_name)

  def get_status_info (self, connected):
    return 'device connected' if connected else 'device not connected'

class gpsd_process (threading.Thread):
  def __init__(self, device):
    threading.Thread.__init__(self)
    self.device = device
    self.queue = Queue.Queue()

  def boot (self):
    self.set_rate()
    command = 'gpsd -b -N -n -D 2 %s' % self.device
    self.p = subprocess.Popen(command.split(), stderr=subprocess.PIPE)

  def run (self):
    self.boot()

    while True:
      line = self.p.stderr.readline()
      if line:
        logging.debug(line.strip())
        self.queue.put(line.strip())
      else:
        break

  def terminate (self):
    os.kill(self.p.pid, signal.SIGTERM)

  #this isn't a good idea; it tends to crash gpsd and/or make it behave strangely
  def flash (self):
    self.set_rate()
    os.kill(self.p.pid, signal.SIGHUP)

  def output (self):
    lines = []
    while True:
      try:
        lines.append(self.queue.get(False))
      except Queue.Empty:
        break
    return lines

  def set_rate (self, baud=BAUD):
    command = 'stty -F %s %d' % (self.device, baud)
    subprocess.Popen(command.split(), stderr=subprocess.PIPE)


class gpsd (monitor_thread):
  def __init__ (self, device, poll_interval=.2):
    monitor_thread.__init__(self, poll_interval)
    self.device = device
    self.gpsd = None

  def get_status (self, status):
    if status == None:
      status = {'up': None, 'online': None, 'speed': None}

    if self.gpsd:
      status['up'] = True
      
      output = self.gpsd.output()
      for ln in output:
        if 'device open failed' in ln:
          status['online'] = False
        elif 'GPS is offline' in ln:
          status['online'] = False
        elif 'opened GPS' in ln:
          status['online'] = True
        else:
          m = re.search('speed +([0-9]+)', ln)
          if m:
            status['speed'] = int(m.group(1))
    else:
      status['up'] = False

    return status

  def get_status_info (self, status):
    if status['up']:
      if status['online'] == None:
        msg = 'gpsd up; searching for device...'
      elif status['online']:
        msg = 'gpsd online'
      else:
        msg = 'gpsd up; device offline'

      if status['online'] and status['speed'] != BAUD:
        msg += '; ' + ('slow! (%.1f)' % (status['speed']/1000.) if status['speed'] else 'speed unknown')

      return msg
    else:
      return 'gpsd not up'

  def start_gpsd (self):
    if self.gpsd == None or not self.gpsd.isAlive():
      self.gpsd = gpsd_process(self.device)
      self.gpsd.start()
    else:
      logging.warn('gpsd already running')

  def stop_gpsd (self):
    if self.gpsd != None and self.gpsd.isAlive():
      self.gpsd.terminate()

      #wait for process to terminate
      for i in range(0, 100):
        if not self.gpsd.isAlive():
          break
        time.sleep(0.1)

      if self.gpsd.isAlive():
        logging.error('gpsd didn\'t terminate after lengthy wait!')
    else:
      logging.warn('gpsd not running')

  def restart_gpsd (self):
    self.stop_gpsd()
    self.start_gpsd()

  #this isn't a good idea; it tends to crash gpsd and/or make it behave strangely
  def flash_gpsd (self):
    self.gpsd.flash()

  def cleanup (self):
    self.stop_gpsd()

class gpsgen_loader (threading.Thread):
  SERVER_RETRY_WAIT = 5.
  LISTENER_RETRY_WAIT = 5.

  def __init__(self):
    threading.Thread.__init__(self)
    
    self.up = True

    self.server = None
    self.server_retry_at = None
    self.dispatcher = None
    self.listener = None
    self.listener_retry_at = None
    self.listener_term = False

  def run (self):
    while not self.server and self.up:
      try:
        self.server_retry_at = None
        self.server = gpslistener.gps_server()
        self.server.start()
      except socket.error:
        self.server_retry_at = time.time() + self.SERVER_RETRY_WAIT
        self.interruptable_wait(self.SERVER_RETRY_WAIT)

    if self.server:
      self.dispatcher = gpslistener.gps_dispatcher(self.server)
      self.dispatcher.start()

    time.sleep(0.5)
    while self.up:
      while not self.listener and self.up:
        try:
          self.listener_retry_at = None
          self.listener = gpslistener.gps_listener(self.dispatcher.queue)
          self.listener.start()
        except gpslistener.linesocket.CantConnect, e:
          self.listener_retry_at = time.time() + self.LISTENER_RETRY_WAIT
          self.interruptable_wait(self.LISTENER_RETRY_WAIT)

      while self.up:
        if not self.listener.isAlive():
          self.listener = None
          self.listener_term = True
          self.listener_retry_at = time.time() + self.LISTENER_RETRY_WAIT
          self.interruptable_wait(self.LISTENER_RETRY_WAIT)
          self.listener_term = False
          break

    for t in [self.listener, self.dispatcher, self.server]:
      if t != None and t.isAlive():
        t.terminate()

  def interruptable_wait (self, n, inc=.3):
    k = 0.
    while self.up and k < n - 1.0e-9:
      time.sleep(min(inc, n - k))
      k += inc

  def terminate (self):
    self.up = False

class gpsgen (monitor_thread):
  def __init__(self, poll_interval=.3):
    monitor_thread.__init__(self, poll_interval)
    self.loader = None

  def get_status (self, stat):
    return 6

  def get_status_info (self, _):
    l = self.loader
    if l == None:
      return 'listener not up'
    elif l.server == None:
      if l.server_retry_at == None:
        return 'loading dispatcher...'
      else:
        return 'dispatcher: can\'t bind port; retry in %d' % int(math.ceil(l.server_retry_at - time.time()))
    elif l.listener == None:
      if l.listener_retry_at == None:
        return 'loading listener...'
      else:
        return '%s; retry in %d' % ('gpsd terminated' if l.listener_term else 'can\'t connect to gpsd', int(math.ceil(l.listener_retry_at - time.time())))
    else:
      since_fix = time.time() - l.dispatcher.last_fix_at if l.dispatcher.last_fix_at else None
      since_ping = time.time() - l.listener.last_data_at if l.listener.last_data_at else None

      if l.listener.sirf_alert:
        return 'gps is in SiRF mode!'
      elif since_ping == None:
        return 'no data yet from gpsd'
      elif since_fix != None and since_fix < 5.:
        return 'lookin\' good!'
      elif since_ping > 20.:
        return 'no data from gpsd in %d' % int(since_ping)
      else:
        msg = ('no fix in %d' % int(since_fix)) if since_fix != None else 'no fix yet'
        msg += '; '
        if l.listener.sat_info != None:
          ttl = len(l.listener.sat_info)
          nvg = len([s for s in l.listener.sat_info.values() if s['snr'] >= 40.])
          ng = len([s for s in l.listener.sat_info.values() if s['snr'] >= 30. and s['snr'] < 40.])
          nb = len([s for s in l.listener.sat_info.values() if s['snr'] >= 20. and s['snr'] < 30.])
          nvb = len([s for s in l.listener.sat_info.values() if s['snr'] > 0. and s['snr'] < 20.])
          msg += '%d sats %dvg/%dg/%db/%dvb' % (ttl, nvg, ng, nb, nvb)
        else:
          msg += 'no sat info'
        return msg

  def load (self):
    self.loader = gpsgen_loader()
    self.loader.start()

  def cleanup (self):
    if self.loader != None:
      self.loader.terminate()



def loader ():
  curses.wrapper(loader_curses)

def loader_curses (w):
  curses.curs_set(0)
  w.nodelay(1)

  dw = device_watcher('/dev/ttyUSB0')
  dw.start()

  gpsdw = gpsd('/dev/ttyUSB0')
  gpsdw.start()
  gpsdw.start_gpsd()

  gpsliw = gpsgen()
  gpsliw.start()
  gpsliw.load()

  up = True
  while up:
    try:
      println(w, fmt_line('GPS Device', dw.status()), 1, 2)
      println(w, fmt_line('GPS Daemon', gpsdw.status()), 2, 2)
      println(w, fmt_line('GPS Listener', gpsliw.status()), 3, 2)

      println(w, 'ESC:   quit', 8, 2)
      println(w, 'F2:    reboot gpsd', 9, 2)

      try:
        key = w.getkey()
      except:
        key = ''

      if key in ('\x1b', '^['):
        up = False
      elif key == 'KEY_F(2)':
        restart(gpsdw)
#      elif key == 'KEY_F(3)':
#        flash(gpsdw, gpsliw)

      time.sleep(0.01)
    except KeyboardInterrupt:
      up = False

  dw.terminate()
  gpsdw.terminate()
  gpsliw.terminate()

def restart (gpsdw):
  gpsdw.restart_gpsd()

#def flash (gpsdw, gpsliw):
#  gpsdw.flash_gpsd()
#  #trigger listener to reconnect

def fmt_line (header, status):
  HEADER_WIDTH = 20
  STATUS_WIDTH = 45

  if status == None:
    status = '-- no status --'

  return header.ljust(HEADER_WIDTH) + ' [' + status.rjust(STATUS_WIDTH) + ']'

def println (w, str, y, x=0):
  w.addstr(y, x, str)
  w.clrtoeol()
  w.refresh()




if __name__ == "__main__":

  loader()
