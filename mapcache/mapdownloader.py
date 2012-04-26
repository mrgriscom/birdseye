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
import maptile as mt
from downloadmanager import DownloadManager
import settings
import util.util as u
import re

from sqlalchemy.sql.expression import tuple_

HASH_LENGTH = 8 # bytes
BULK_RESOLUTION = 100

def null_digest():
    return '00' * HASH_LENGTH

def tile_url((zoom, x, y), layer):
    L = settings.LAYERS[layer]
    if '_tileurl' not in L:
        L['_tileurl'] = precompile_tile_url(L['tile_url'])
    return L['_tileurl'](zoom, x, y)

def precompile_tile_url(template):
    replacements = {
        '{z}': '%(z)d',
        '{x}': '%(x)d',
        '{y}': '%(y)d',
        '{qt}': '%(qt)s',
    }

    shards = []
    shard_match = re.search(r'\{s:([^\}]+)\}', template)
    if shard_match:
        shard_tag = shard_match.group(0)
        shard_spec = shard_match.group(1)

        if '-' in shard_spec:
            min, max = (int(k) for k in shard_spec.split('-'))
            shards = range(min, max + 1)
        else:
            shards = list(shard_spec)
        replacements[shard_tag] = '%(shard)s'

    has_qt = '{qt}' in template

    fmtstr = reduce(lambda s, (old, new): new.join(s.split(old)), replacements.iteritems(), template)
    def _url(z, x, y):
        if shards:
            shard = shards[(x + y) % len(shards)]
        if has_qt:
            qt = u.to_quadindex(z, x, y)
        return fmtstr % locals()
    return _url

def query_tiles(sess, layer, chunk, ignore_missing):
    q = conn.query(mt.Tile).filter(mt.Tile.layer == layer).filter(tuple_(mt.Tile.z, mt.Tile.x, mt.Tile.y).in_(list(chunk)))
    if ignore_missing:
        q = q.filter(mt.Tile.uuid != null_digest())
    return set((t.z, t.x, t.y) for t in q)

def existing_tiles(sess, tiles, ignore_missing=False, chunk_size=BULK_RESOLUTION):
    for chunk in u.chunker(tiles, chunk_size):
        yield (query_tiles(sess, chunk, ignore_missing), len(chunk))

def random_walk_level(tiles, window=10):
    target = None
    while tiles:
        if not target:
            target = u.rand_elem(tiles)
        else:
            metric = lambda t: u.manhattan_dist(target[1:], t[1:])
            closest = min(metric(t) for t in tiles)
            candidates = [t for t in tiles if metric(t) == closest]
            target = random.choice(candidates)

        (xmin, ymin) = [f - window / 2 for f in target[1:]]
        (xmax, ymax) = [f + window - 1 for f in (xmin, ymin)]

        swatch = list(filter_set(tiles, lambda z, x, y: x >= xmin and x <= xmax and y >= ymin and y <= ymax))
        random.shuffle(swatch)
        for t in swatch:
            yield t

def random_walk(tiles):
    zooms = sorted(set(z for z, x, y in tiles))
    for zoom in zooms:
        for t in random_walk_level(filter_set(tiles, lambda z, x, y: z == zoom)):
            yield t

def register_tile(conn, tile, digest):
    # TODO: handle refreshing a tile (update uuid, fetched_on)

    z, x, y = tile
    t = mt.Tile(z=z, x=x, y=y, layer='gmap-map', uuid=digest)

    conn.add(t)
    conn.commit() # TODO: only commit every N?

def process_tile(conn, tile, status, data):
    def digest(data):
        if data is not None:
            return hashlib.sha1(data).hexdigest()[:HASH_LENGTH*2]
        else:
            return null_digest()

    if status in [httplib.OK, httplib.NOT_FOUND]:
        if status == httplib.OK:

            save_result = save_tile(data, digest)
 
            if not save_result:
                return (False, '%s: could not write file' % str(tile))
        else:
            digest = null_digest()
        register_tile(conn, tile, digest)

        return (True, None)
    else:
        if status == None:
            msg = 'Tile %s: unable to download' % str(tile)
        elif status == httplib.FORBIDDEN:
            msg = 'Warning: we may have been banned'
        else:
            msg = 'Unrecognized response code %d' % status
        return (False, msg)

def tile_counts(tiles):
    # todo use map-reduce
    def accumulate(totals, tile):
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
    def __init__    (self, region, depth):
        threading.Thread.__init__(self)

        self.tess = mt.RegionTessellation(region, depth)
        self.est_num_tiles = self.tess.size_estimate()
        self.tiles = set()
        self.count = 0

    def run(self):
        for t in self.tess:
            self.tiles.add(t)
            self.count += 1
        self.est_num_tiles = self.count

    def status(self):
        return (self.count, self.est_num_tiles, 0)

class tile_culler (threading.Thread):
    def __init__(self, tiles, refresh_mode, conn):
        threading.Thread.__init__(self)

        self.conn = conn
        self.refresh_mode = refresh_mode
        self.reftiles = tiles
        self.existtiles = set()
        self.tiles = None

        self.num_tiles = len(tiles)
        self.num_processed = 0

    def run(self):
        if self.refresh_mode == 'always':
            self.num_processed = self.num_tiles
        else:
            for (existing, num_queried) in existing_tiles(self.conn, self.reftiles, self.refresh_mode == 'missing'):
                self.existtiles |= existing
                self.num_processed += num_queried
        self.tiles = self.reftiles - self.existtiles
        self.conn.close()

    def status(self):
        return (self.num_processed, self.num_tiles, 0)

class tile_downloader (threading.Thread):
    def __init__(self, tiles, conn):
        threading.Thread.__init__(self)

        self.tiles = tiles
        self.num_tiles = len(tiles)
        self.errors = Queue.Queue()
        self.dlmgr = DownloadManager([httplib.OK, httplib.NOT_FOUND, httplib.FORBIDDEN], limit=100, errs=self.errors)
        self.dlpxr = self.DownloadProcessor(self.dlmgr.out_queue, lambda i, s, d: process_tile(conn, i, s, d), self.num_tiles, self.errors)

    def run(self):
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

    def status(self):
        return (self.dlpxr.count, self.num_tiles, self.dlpxr.errcount)

    class DownloadProcessor (threading.Thread):
        def __init__(self, queue, processfunc, num_expected=None, errs=None):
            threading.Thread.__init__(self)
            self.up = True

            self.process = processfunc
            self.queue = queue
            self.count = 0
            self.num_expected = num_expected
            self.errcount = 0
            self.errors = errs

        def terminate(self):
            self.up = False

        def run(self):
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

        def done(self):
            if self.num_expected != None:
                return self.count == self.num_expected
            else:
                return self.queue.empty()





def download(region, overlay, max_depth, refresh_mode):
    curses.wrapper(download_curses, region, overlay, max_depth, refresh_mode)

def download_curses(w, region, overlay, max_depth, refresh_mode):
    polygon = Polygon([mt.mercator_to_xy(mt.ll_to_mercator(p)) for p in region.contour(0)])
 
    te = tile_enumerator(polygon, max_depth)
    monitor(w, 0, te, 'Enumerating', 15, 3)

    print_tile_counts(w, tile_counts(te.tiles), 'Tiles in region', 4, 2, max_depth=max_depth)

    tc = tile_culler(te.tiles, refresh_mode, mt.dbsess())
    monitor(w, 1, tc, 'Culling', 15)

    print_tile_counts(w, tile_counts(tc.tiles), 'Tiles to download', 4, 19, max_depth=max_depth)

    td = tile_downloader(tc.tiles, mt.dbsess())
    monitor(w, 2, td, 'Downloading', 15, erry=3)

    try:
        while True:
            pass
    except KeyboardInterrupt:
        pass

def monitor(w, y, thread, caption, width, sf=0, erry=None):
    thread.start()
    while thread.isAlive():
        update_status(w, thread, False, y, caption, width, sf, erry)
        time.sleep(.01)
    update_status(w, thread, True, y, caption, width, sf, erry)

def update_status(w, thread, done, y, caption, width, sf, erry):
    println(w, status(caption, thread.status(), width, sf if not done else 0), y)
 
    if erry != None:
        err = get_error(thread)
        if err:
            println(w, err, erry, 2)

def get_error(thread):
    try:
        return thread.errors.get(False)
    except Queue.Empty:
        return None

def println(w, str, y, x=0):
    w.addstr(y, x, str)
    w.clrtoeol()
    w.refresh()

def status(caption, (k, n, e), width=None, est_sigfig=0):
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
    errstr = '         [Errors: %4d]' % e if e > 0 else ''
    return '%s %s%6.2f%% [%*d/%s]%s' % ((caption + ':').ljust(width + 1), '+' if overflow else ' ',
            100. * ratio, pad, k, max_str, errstr)

def print_tile_counts(w, counts, header, y, x, width=None, zheader='Zoom', max_depth=None):
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
