import threading
import math
import time
from datetime import datetime
import random
import os
import httplib
import Queue
import hashlib
import curses
import psycopg2
from Polygon import *
import maptile
from downloadmanager import DownloadManager
import config

import sys #debug

hash_length = 8

def null_digest ():
  return '00' * hash_length

def tile_url ((zoom, x, y), type):
  if type != 'gmap-map':
    print 'unsupported overlay type!'
    sys.exit()

  return "http://mt%d.google.com/vt/x=%d&y=%d&z=%d" % ((x + y) % 4, x, y, zoom)
  #return "http://mt%d.google.com/mt?x=%d&y=%d&z=%d" % ((x + y) % 4, x, y, zoom)

def dbconn ():
  try:
    return psycopg2.connect(database=config.db)
  except:
    print 'can\'t connect to database'
    sys.exit()

def query_tiles (conn, chunk, ignore_missing):
  qtiles = [(z, x, y, 1) for (z, x, y) in chunk]
  stiles = ['(%s)' % ', '.join([str(f) for f in qt]) for qt in qtiles]
  exclude_clause = ' and uuid != \'%s\'' % null_digest() if ignore_missing else ''
  query = 'select z, x, y, type from tiles where (z, x, y, type) in (%s)%s;' % (', '.join(stiles), exclude_clause)
  
  curs = conn.cursor()
  curs.execute(query)
  rows = curs.fetchall()
  curs.close()

  return set([(z, x, y) for (z, x, y, t) in rows])

def chunker (container, chunk_size):
  chunk = []
  for v in container:
    chunk.append(v)

    if len(chunk) == chunk_size:
      yield chunk
      chunk = []
  if chunk:
    yield chunk

def existing_tiles (conn, tiles, ignore_missing=False, chunk_size=100):
  for chunk in chunker(tiles, chunk_size):
    yield (query_tiles(conn, chunk, ignore_missing), len(chunk))

def filter_set (s, filter, remove=True):
  t = set()
  for e in s:
    if filter(e):
      t.add(e)
  if remove:
    for e in t:
      s.remove(e)
  return t

def rand_elem (s):
  r = random.randint(0, len(s) - 1)
  for (i, e) in enumerate(s):
    if i == r:
      return e

def max_elem (s, val=lambda x: x):
  max_val = None
  results = []
  for e in s:
    val_e = val(e)
    if max_val == None or val_e > max_val:
      results = [e]
      max_val = val_e
    elif val_e == max_val:
      results.append(e)
  return results

def manhattan_dist ((x0, y0), (x1, y1)):
  return abs(x0 - x1) + abs(y0 - y1)

def random_walk_level (tiles, window=10):
  target = None
  while tiles:
    if not target:
      target = rand_elem(tiles)
    else:
      candidates = max_elem(tiles, lambda t: -manhattan_dist(target[1:3], t[1:3]))
      target = random.choice(candidates)

    (xmin, ymin) = [f - window / 2 for f in target[1:3]]
    (xmax, ymax) = [f + window - 1 for f in (xmin, ymin)]

    swatch = list(filter_set(tiles, lambda (z, x, y): x >= xmin and x <= xmax and y >= ymin and y <= ymax))
    random.shuffle(swatch)
    for t in swatch:
      yield t

def random_walk (tiles):
  zooms = [z for (z, count) in enumerate(tile_counts(tiles)) if count > 0]
  for zoom in zooms:
    for t in random_walk_level(filter_set(tiles, lambda (z, x, y): z == zoom)):
      yield t

def save_tile (data, digest):
  path = config.tile_dir
  if path[-1] != '/':
    path += '/'

  #buckets = [2, 4]
  buckets = [3]
  for i in buckets:
    path += digest[:i] + '/'
    if not os.path.exists(path):
      os.mkdir(path)

  path += digest + '.png'
  if not os.path.exists(path):
    try:
      f = open(path, 'w')
      f.write(data)
      f.close()
    except IOError:
      return False
  return True

c = 0 #debug
def register_tile (conn, tile, digest):
  check_query = 'select uuid from tiles where (z, x, y, type) = (%(z)s, %(x)s, %(y)s, %(type)s);'
  insert_query = 'insert into tiles (z, x, y, type, uuid, fetched_on) values (%(z)s, %(x)s, %(y)s, %(type)s, %(uuid)s, %(now)s);'
  update_query = 'update tiles set uuid = %(uuid)s, fetched_on = %(now)s where (z, x, y, type) = (%(z)s, %(x)s, %(y)s, %(type)s);'

  (z, x, y) = tile
  vals = dict(z=z, x=x, y=y, type=1, uuid=digest, now=datetime.utcnow())

  curs = conn.cursor()
  curs.execute(check_query, vals)
  if curs.rowcount > 0:
    curs.execute(update_query, vals)
  else:
    curs.execute(insert_query, vals)

  #global c #debug
  #c += 1 #debug
  #if c % 200 == 0: #debug
  conn.commit()
  curs.close()

def process_tile (conn, tile, status, data):
  if status in [httplib.OK, httplib.NOT_FOUND]:
    if status == httplib.OK:
      #tst1 = time.time() #debug

      digest = hashlib.sha1(data).hexdigest()[0:hash_length*2]
      save_result = save_tile(data, digest)
 
      #ted1 = time.time() #debug

      if not save_result:
        return (False, '%s: could not write file' % str(tile))
    else:
      digest = null_digest()
#    tst2 = time.time() #debug
    register_tile(conn, tile, digest)
#    ted2 = time.time() #debug

#    sys.stderr.write('process: %f %f\n' % (ted1-tst1 if ted1 != None else -1, ted2-tst2)) #debug
    return (True, None)
  else:
    if status == None:
      msg = 'Tile %s: unable to download' % str(tile)
    elif status == httplib.FORBIDDEN:
      msg = 'Warning: we may have been banned'
    else:
      msg = 'Unrecognized response code %d' % status
    return (False, msg)

def tile_counts (tiles):
  def accumulate (totals, tile):
    z = tile[0]
    if z not in totals:
      totals[z] = 0
    totals[z] += 1
    return totals

  totals = reduce(accumulate, tiles, {})
  counts = [0] * ((max(totals.keys()) if totals else -1) + 1)
  for z in totals.keys():
    counts[z] = totals[z]
  return counts

class tile_enumerator (threading.Thread):
  def __init__  (self, region, depth):
    threading.Thread.__init__(self)

    self.tess = maptile.region_tessellation(region, depth)
    self.est_num_tiles = self.tess.size_estimate()
    self.tiles = set()
    self.count = 0

  def run (self):
    for t in self.tess:
      self.tiles.add(t)
      self.count += 1
    self.est_num_tiles = self.count

  def status (self):
    return (self.count, self.est_num_tiles, 0)

class tile_culler (threading.Thread):
  def __init__ (self, tiles, refresh_mode, conn):
    threading.Thread.__init__(self)

    self.conn = conn
    self.refresh_mode = refresh_mode
    self.reftiles = tiles
    self.existtiles = set()
    self.tiles = None

    self.num_tiles = len(tiles)
    self.num_processed = 0

  def run (self):
    if self.refresh_mode == 'always':
      self.num_processed = self.num_tiles
    else:
      for (existing, num_queried) in existing_tiles(self.conn, self.reftiles, self.refresh_mode == 'missing'):
        self.existtiles |= existing
        self.num_processed += num_queried
    self.tiles = self.reftiles - self.existtiles
    self.conn.close()

  def status (self):
    return (self.num_processed, self.num_tiles, 0)

class tile_downloader (threading.Thread):
  def __init__ (self, tiles, conn):
    threading.Thread.__init__(self)

    self.tiles = tiles
    self.num_tiles = len(tiles)
    self.errors = Queue.Queue()
    self.dlmgr = DownloadManager([httplib.OK, httplib.NOT_FOUND, httplib.FORBIDDEN], limit=100, errs=self.errors)
    self.dlpxr = self.DownloadProcessor(self.dlmgr.out_queue, lambda i, s, d: process_tile(conn, i, s, d), self.num_tiles, self.errors)

  def run (self):
    self.dlmgr.start()
    self.dlpxr.start()

    for t in random_walk(self.tiles):
      self.dlmgr.enqueue((t, tile_url(t, 'gmap-map')))

    while not self.dlmgr.done():
      pass
    self.dlmgr.terminate()

    while not self.dlpxr.done():
      pass
    self.dlpxr.terminate()
    self.dlpxr.join()

  def status (self):
    return (self.dlpxr.count, self.num_tiles, self.dlpxr.errcount)

  class DownloadProcessor (threading.Thread):
    def __init__ (self, queue, processfunc, num_expected=None, errs=None):
      threading.Thread.__init__(self)
      self.up = True

      self.process = processfunc
      self.queue = queue
      self.count = 0
      self.num_expected = num_expected
      self.errcount = 0
      self.errors = errs

    def terminate (self):
      self.up = False

    def run (self):
      while self.up:
        try:
          (item, status, data) = self.queue.get(True, 0.05)
          (success, msg) = self.process(item, status, data)
          self.count += 1
          if not success:
            self.errcount += 1
          if msg:
            self.errors.put(msg)
        except Queue.Empty:
          pass
        except Exception, e:
          if self.errors != None:
            self.errors.put('Unexpected exception in download processor thread: ' + str(e))

    def done (self):
      if self.num_expected != None:
        return self.count == self.num_expected
      else:
        return self.queue.empty()





def download (region, overlay, max_depth, refresh_mode):
  curses.wrapper(download_curses, region, overlay, max_depth, refresh_mode)

def download_curses (w, region, overlay, max_depth, refresh_mode):
  polygon = Polygon([maptile.mercator_to_xy(maptile.ll_to_mercator(p)) for p in region.contour(0)])
 
  te = tile_enumerator(polygon, max_depth)
  monitor(w, 0, te, 'Enumerating', 15, 3)

  print_tile_counts(w, tile_counts(te.tiles), 'Tiles in region', 4, 2, max_depth=max_depth)

  tc = tile_culler(te.tiles, refresh_mode, dbconn())
  monitor(w, 1, tc, 'Culling', 15)

  print_tile_counts(w, tile_counts(tc.tiles), 'Tiles to download', 4, 19, max_depth=max_depth)

  td = tile_downloader(tc.tiles, dbconn())
  monitor(w, 2, td, 'Downloading', 15, erry=3)

  try:
    while True:
      pass
  except KeyboardInterrupt:
    pass

def monitor (w, y, thread, caption, width, sf=0, erry=None):
  thread.start()
  while thread.isAlive():
    update_status(w, thread, False, y, caption, width, sf, erry)
    time.sleep(.01)
  update_status(w, thread, True, y, caption, width, sf, erry)

def update_status (w, thread, done, y, caption, width, sf, erry):
  println(w, status(caption, thread.status(), width, sf if not done else 0), y)
 
  if erry != None:
    err = get_error(thread)
    if err:
      println(w, err, erry, 2)

def get_error (thread):
  try:
    return thread.errors.get(False)
  except Queue.Empty:
    return None

def println (w, str, y, x=0):
  w.addstr(y, x, str)
  w.clrtoeol()
  w.refresh()

def status (caption, (k, n, e), width=None, est_sigfig=0):
  width = width if width else len(caption)

  ratio = float(k) / n if n > 0 else 1.
  overflow = ratio > 1.
  ratio = min(ratio, 1.)

  if est_sigfig > 0:
    digits = int(math.ceil(math.log10(n)))
    trunc = max(digits - est_sigfig, 0)
    n = int(round(n, -trunc))
    max_str = '%d (est)' % n
  else:
    max_str = '%d' % n

  pad = len(str(n))
  errstr = '     [Errors: %4d]' % e if e > 0 else ''
  return '%s %s%6.2f%% [%*d/%s]%s' % ((caption + ':').ljust(width + 1), '+' if overflow else ' ',
      100. * ratio, pad, k, max_str, errstr)

def print_tile_counts (w, counts, header, y, x, width=None, zheader='Zoom', max_depth=None):
  if not width:
    width = max(len(header), 10)
  zwidth = len(zheader)

  maxz = max(len(counts), max_depth + 1 if max_depth != None else 0)

  w.addstr(y, 0, zheader)
  w.addnstr(y, zwidth + x, header.rjust(width), width)
  for i in range(0, maxz):
    w.addstr(y + 1 + i, 0, '%*d' % (zwidth, i))
    w.addstr(y + 1 + i, zwidth + x, '%*d' % (width, counts[i] if i < len(counts) else 0))

  w.refresh()
