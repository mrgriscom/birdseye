import threading
import curses
import time
import os
import sys
import subprocess
import signal
import Queue
import re

DEVICE = '/dev/ttyUSB0'
BAUD = 57600
POLL_INTERVAL = 1.

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
        self.queue.put(line.strip())
      else:
        break

  def terminate (self):
    os.kill(self.p.pid, signal.SIGTERM)

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
      print 'gpsd already running'

  def stop_gpsd (self):
    if self.gpsd != None and self.gpsd.isAlive():
      self.gpsd.terminate()
    else:
      print 'gpsd not running'

  def restart_gpsd (self):
    self.stop_gpsd()
    self.start_gpsd()

  def flash_gpsd (self):
    self.gpsd.flash()

  def cleanup (self):
    self.stop_gpsd()



def loader ():
  curses.wrapper(loader_curses)

def loader_curses (w):
  curses.curs_set(0)

  dw = device_watcher('/dev/ttyUSB0')
  dw.start()

  gpsdw = gpsd('/dev/ttyUSB0')
  gpsdw.start()
  gpsdw.start_gpsd()

  try:
    while True:
      println(w, fmt_line('GPS Device', dw.status()), 1, 2)
      println(w, fmt_line('GPS Daemon', gpsdw.status()), 2, 2)

      time.sleep(0.01)
  except KeyboardInterrupt:
    pass

  dw.terminate()
  gpsdw.terminate()

def fmt_line (header, status):
  HEADER_WIDTH = 20
  STATUS_WIDTH = 35

  if status == None:
    status = '-- no status --'

  return header.ljust(HEADER_WIDTH) + ' [' + status.rjust(STATUS_WIDTH) + ']'

def println (w, str, y, x=0):
  w.addstr(y, x, str)
  w.clrtoeol()
  w.refresh()




if __name__ == "__main__":

  loader()
