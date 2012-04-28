import threading
import math
import time
from datetime import datetime, timedelta
import random
import os
import httplib
import Queue
import hashlib
from Polygon import *
import maptile as mt
from downloadmanager import DownloadManager
import settings
import util.util as u
import re
import collections
import logging

from sqlalchemy.sql.expression import tuple_, or_, and_

HASH_LENGTH = 8 # bytes
CULL_RESOLUTION = 100
COMMIT_INTERVAL = 40

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

def query_tiles(sess, layer, chunk, refresh_cutoff, refresh_cutoff_missing):
    q = sess.query(mt.Tile).filter(mt.Tile.layer == layer).filter(tuple_(mt.Tile.z, mt.Tile.x, mt.Tile.y).in_(list(chunk)))
    def cutoff_criteria():
        if refresh_cutoff is not None:
            yield and_(mt.Tile.uuid != null_digest(), mt.Tile.fetched_on > refresh_cutoff)
        if refresh_cutoff_missing is not None:
            yield and_(mt.Tile.uuid == null_digest(), mt.Tile.fetched_on > refresh_cutoff_missing)
    coc = list(cutoff_criteria())
    if coc:
        q = q.filter(or_(*coc))
    return set((t.z, t.x, t.y) for t in q)

def existing_tiles(sess, tiles, layer, refresh_window=None, refresh_window_missing=None, chunk_size=CULL_RESOLUTION):
    def cutoff(threshold):
        return datetime.now() - threshold if threshold is not None else None

    refresh_cutoff = cutoff(refresh_window)
    refresh_cutoff_missing = cutoff(refresh_window_missing)

    for chunk in u.chunker(tiles, chunk_size):
        yield (query_tiles(sess, layer, chunk, refresh_cutoff, refresh_cutoff_missing), len(chunk))

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

        swatch = list(u.set_filter(tiles, lambda (z, x, y): x >= xmin and x <= xmax and y >= ymin and y <= ymax))
        random.shuffle(swatch)
        for t in swatch:
            yield t

def random_walk(tiles):
    zooms = sorted(set(z for z, x, y in tiles))
    for zoom in zooms:
        for t in random_walk_level(u.set_filter(tiles, lambda (z, x, y): z == zoom)):
            yield t

def register_tile(dbpush, tile, layer, data, hashfunc):
    z, x, y = tile
    t = mt.Tile(layer=layer, z=z, x=x, y=y)
    t.save(data, hashfunc)
    dbpush(t)

def process_tile(dbpush, tile, layer, status, data):
    def digest(data):
        if data is not None:
            return hashlib.sha1(data).hexdigest()[:HASH_LENGTH*2]
        else:
            return null_digest()

    if status in [httplib.OK, httplib.NOT_FOUND]:
        try:
            register_tile(dbpush, tile, layer, data if status == httplib.OK else None, digest)
            return (True, None)
        except IOError:
            return (False, '%s: could not write file' % str(tile))
    else:
        if status == None:
            msg = 'Tile %s: download error %s' % (str(tile), data)
        elif status == httplib.FORBIDDEN:
            msg = 'Warning: we may have been banned'
        else:
            msg = 'Unrecognized response code %d' % status
        return (False, msg)

def tile_counts(tiles):
    totals = collections.defaultdict(lambda: 0, u.map_reduce(tiles, lambda (z, x, y): [(z,)], len))
    max_zoom = max(totals.keys()) if totals else -1
    return [totals[z] for z in range(max_zoom + 1)]

class TileEnumerator(threading.Thread):
    def __init__(self, region, depth):
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

class TileCuller(threading.Thread):
    def __init__(self, tiles, layer, refresh_window, refresh_window_missing, sess):
        threading.Thread.__init__(self)

        self.tiles = tiles
        self.existing_tiles = set()

        if refresh_window == timedelta(0) and refresh_window_missing == timedelta(0):
            self.existing_tile_stream = None
        else:
            self.existing_tile_stream = existing_tiles(sess, tiles, layer, refresh_window, refresh_window_missing)

        self.num_tiles = len(tiles)
        self.num_processed = 0

    def run(self):
        if self.existing_tile_stream:
            for existing, num_queried in self.existing_tile_stream:
                self.existing_tiles |= existing
                self.num_processed += num_queried
        else:
            self.num_processed = self.num_tiles

        self.tiles = self.tiles - self.existing_tiles

    def status(self):
        return (self.num_processed, self.num_tiles, 0)

class TileDownloader(threading.Thread):
    def __init__(self, tiles, layer, sess):
        threading.Thread.__init__(self)

        self.tiles = tiles
        self.layer = layer

        self.num_tiles = len(tiles)

        self.error_count = 0
        self.last_error = None

        self.dlmgr = DownloadManager([httplib.OK, httplib.NOT_FOUND, httplib.FORBIDDEN], limit=100)

        self.dbsess = TileDB(sess, COMMIT_INTERVAL)
        def process(key, status, data):
            return process_tile(self.dbsess.push, key, self.layer, status, data)
        self.dlpxr = DownloadProcessor(self.dlmgr, process, self.num_tiles, self.onerror)

    def run(self):
        self.dlmgr.start()
        self.dlpxr.start()

        for t in random_walk(self.tiles):
            self.dlmgr.enqueue((t, tile_url(t, self.layer)))

        self.dlpxr.join()
        self.dbsess.commit()

        self.dlmgr.terminate()
        self.dlmgr.join()

    def onerror(self, msg):
        self.error_count += 1
        self.last_error = msg

    def status(self):
        return (self.dlpxr.count, self.num_tiles, self.error_count)

class DownloadProcessor(threading.Thread):
    def __init__(self, dlmgr, processfunc, num_expected=None, onerror=lambda m: None):
        threading.Thread.__init__(self)
        self.up = True

        self.process = processfunc
        self.dlmgr = dlmgr
        self.num_expected = num_expected

        self.count = 0
        self.onerror = onerror
 
    def terminate(self):
        self.up = False

    def run(self):
        try:
            while self.up and not self.done():
                item = self.dlmgr.fetch()
                if not item:
                    continue
                
                success, msg = self.process(*item)
                self.count += 1
                if not success:
                    self.onerror(msg)
        except:
            logging.exception('unexpected exception in download processor thread')

    def done(self):
        return self.count == (self.num_expected if self.num_expected is not None else -1)

class TileDB(object):
    def __init__(self, sess, limit=1):
        self.sess = sess
        self.limit = limit

        self.pending = []

    def push(self, tile):
        self.pending.append(tile)
        if len(self.pending) >= self.limit:
            self.commit()

    def commit(self):
        existing = dict((t.pk(), t) for t in self.sess.query(mt.Tile).filter(tuple_(mt.Tile.layer, mt.Tile.z, mt.Tile.x, mt.Tile.y).in_(t.pk() for t in self.pending)))

        old_uuids = set()
        for t in self.pending:
            t_old = existing.get(t.pk())
            if t_old:
                if t_old.uuid != t.uuid:
                    old_uuids.add(t_old.uuid)
                    t_old.uuid = t.uuid
            else:
                self.sess.add(t)

        self.sess.commit()

        for uuid in old_uuids:
            if uuid == null_digest():
                continue

            if not self.sess.query(mt.Tile).filter(mt.Tile.uuid == uuid).count():
                # no tile references this file uuid anymore
                os.remove(mt.Tile(uuid=uuid).path())

        self.pending = []


