import gpslistener
import psycopg2
import sys
import logging
import threading
import time

def insert_query (table, column_mapping):
  columns = ', '.join(column_mapping.keys())
  fields = ', '.join(map(lambda f: '%%(%s)s' % f, column_mapping.values()))
  return 'insert into %s (%s) values (%s);' % (table, columns, fields)

insert_gps_fix = insert_query('gps_log', {
  'gps_time': 'time',
  'system_time': 'systime',
  'latitude': 'lat',
  'longitude': 'lon',
  'altitude': 'alt',
  'speed': 'speed',
  'heading': 'heading',
  'climb': 'climb',
  'err_horiz': 'h_error',
  'err_vert': 'v_error',
  'type_of_fix': 'fix_type',
  'comment': 'comment'
})



class gpslogger (threading.Thread):
  MAX_BUFFER = 60       #points
  COMMIT_INTERVAL = 180 #seconds
  DISPATCH_RETRY_WAIT = 3.

  def __init__(self):
    threading.Thread.__init__(self)
    self.up = True

    self.dbconn = None
    self.gps = None
    self.dispatch_retry_at = None
    self.buffer = []
    self.buffer_age = None

  def run (self):
    try:
      self.dbconn = psycopg2.connect(database='geoloc')
    except:
      logging.exception('gpslogger can\'t connect to db')
      return

    while not self.gps and self.up:
      try:
        self.dispatch_retry_at = None
        self.gps = gpslistener.gps_subscription()
      except gpslistener.linesocket.CantConnect:
        self.dispatch_retry_at = time.time() + self.DISPATCH_RETRY_WAIT
        self.interruptable_wait(self.DISPATCH_RETRY_WAIT)

    if self.gps:
      while self.up:
        try:
          data = self.gps.get_fix()
          if data != None:
            self.buffer.append(data)
            if self.buffer_age == None:
              self.buffer_age = time.time()

          if len(self.buffer) >= self.MAX_BUFFER or (self.buffer_age != None and (time.time() - self.buffer_age) > self.COMMIT_INTERVAL):
            self.flushbuffer()
        except gpslistener.linesocket.BrokenConnection:
          logging.warn('gpslogger: broken connection; exiting...')
          self.terminate()
        except:
          logging.exception('error in main logger loop')

      self.flushbuffer()
      self.gps.unsubscribe()

    self.dbconn.close()

  def terminate (self):
    self.up = False

  def flushbuffer(self):
    curs = self.dbconn.cursor()
    for p in self.buffer:
      try:
        curs.execute(insert_gps_fix, p)
      except:
        logging.exception('error committing fix: ' + str(p))
    self.dbconn.commit()
    curs.close()
    self.buffer = []
    self.buffer_age = None

  def interruptable_wait (self, n, inc=.3):
    k = 0.
    while self.up and k < n - 1.0e-9:
      time.sleep(min(inc, n - k))
      k += inc

