import threading
import curses
import time
import os
import sys

#class monitor_thread (threading.Thread):
#  def __init__ (self):
#    threading.Thread.__init__(self)
#    self.up = True
#    self.lock = threading.Lock()
#  
#  def terminate (self):
#    self.up = False
#
#  def run (self):
#    while self.up:
#      do_body()
#
#      time.sleep(self.poll_interval)
#
#  def set
#
#  def status

class device_watcher (threading.Thread):
  def __init__ (self, device_name):
    threading.Thread.__init__(self)
    self.up = True
    self.lock = threading.Lock()
    self.POLL_INTERVAL = 1.

    self.device_name = device_name
    self.connected = None

  def terminate (self):
    self.up = False

  def run (self):
    while self.up:
      self.lock.acquire()
      self.connected = os.path.exists(self.device_name)
      self.lock.release()

      time.sleep(self.POLL_INTERVAL)

  def status (self):
    self.lock.acquire()

    if self.connected != None:
      stat = 'device connected' if self.connected else 'device not connected'
    else:
      stat = None

    self.lock.release()
    return stat

def loader ():
  curses.wrapper(loader_curses)

def loader_curses (w):
  dw = device_watcher('/dev/ttyUSB0')
  dw.start()

  try:
    while True:
      println(w, fmt_line('GPS Device', dw.status()), 1, 2)

      time.sleep(0.01)
  except KeyboardInterrupt:
    pass

  dw.terminate()

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
