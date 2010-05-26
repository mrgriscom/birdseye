import threading
import os
import time
from util import geodesy
from gps import gpslistener
import sys

class tracker(threading.Thread):
  def __init__ (self, demo=None):
    threading.Thread.__init__(self)
    self.up = True

    self.loclock = threading.Lock()

    if demo == None:
      self.demo = False
      self.p = None
      self.v = None
      self.t = None

      self.gps = None
      while not self.gps:
        try:
          self.gps = gpslistener.gps_subscription()
        except gpslistener.linesocket.CantConnect:
          print 'cannot connect to gps dispatcher; retrying in 5...'
          time.sleep(5)
    else:
      self.demo = True
      self.update_loc(*demo)

  def terminate (self):
    self.up = False

  def run (self):
    while self.up:
      if not self.demo:
        data = self.gps.get_fix()
        if data != None:
          p = (data['lat'], data['lon'])
          v = (data['speed'],
                geodesy.anglenorm(data['heading']) if data['heading'] != None else None)
          self.update_loc(p, v)

  def update_loc (self, p, v):
    self.loclock.acquire()

    self.p = p
    if v[0] != None and v[1] != None:
      self.v = v
    self.t = time.time()

    self.loclock.release()

  def get_loc (self):
    self.loclock.acquire()

    if self.p == None:
      p = None
      v = None
      dt = None
    else:
      dt = time.time() - self.t
      p = geodesy.plot(self.p, self.v[1], self.v[0] * dt)

      v = self.v
      bearing = geodesy.bearing(p, self.p)
      if bearing != None:
        if abs(geodesy.anglenorm(geodesy.bearing(self.p, p)) - v[1]) < 1.0e-3:
          bearing += 180.
        v = (v[0], geodesy.anglenorm(bearing))

    self.loclock.release()

    return (p, v, dt)



if __name__ == "__main__":

  tracker = tracker() #((0.0, 0.0), (100000., -110.)))
  tracker.start()

  try:
    while True:
      print tracker.get_loc()
      time.sleep(0.1)
  except KeyboardInterrupt:
    pass

  tracker.terminate()
