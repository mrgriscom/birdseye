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
CULL_RESOLUTION = 100 # tiles

def null_digest():
    """special digest for tiles with no data (e.g., missing tiles)"""
    return '00' * HASH_LENGTH

def tile_url((zoom, x, y), layer):
    """download url for a tile and layer"""
    L = settings.LAYERS[layer]
    if '_tileurl' not in L:
        urlgen = L['tile_url']
        if hasattr(urlgen, '__call__'):
            urlgen = init_tile_url(layer, urlgen)
            if urlgen is None:
                # init failure; don't set _tile_url so init will be attempted
                # again on next call (not useful for bulk download but yes for
                # on-the-fly tile service)
                return 'http://-fail.blog' # trigger 'domain not known' error

        if hasattr(urlgen, '__call__'):
            def format_url(z, x, y):
                template = urlgen(z, x, y)
                compiled = precompile_tile_url(template, L.get('file_type'))
                return compiled(z, x, y)
            L['_tileurl'] = format_url
        else:
            L['_tileurl'] = precompile_tile_url(urlgen, L.get('file_type'))
    return L['_tileurl'](zoom, x, y)

def init_tile_url(layer, urlinit):
    try:
        return urlinit()
    except:
        logging.exception('error initializing url format for layer [%s]' % layer)

def precompile_tile_url(template, file_type):
    """precompile the tile url format into a form that can be templated efficiently"""
    replacements = {
        '{z}': '%(z)d',
        '{x}': '%(x)d',
        '{y}': '%(y)d',
        '{-y}': '%(inv_y)d',
        '{type}': file_type or '',
    }

    # protect '%' in original string as we convert to format string
    template = '%%'.join(template.split('%'))

    shards = []
    shard_match = re.search(r'\{s:(?P<spec>[^\}]+)\}', template)
    if shard_match:
        shard_tag = shard_match.group(0)
        shard_spec = shard_match.group('spec')

        if '-' in shard_spec:
            min, max = (int(k) for k in shard_spec.split('-'))
            shards = range(min, max + 1)
        else:
            shards = list(shard_spec)
        replacements[shard_tag] = '%(shard)s'

    make_qt = None
    qt_match = re.search(r'\{qt(:(?P<spec>[^\}]+))?\}', template)
    if qt_match:
        qt_tag = qt_match.group(0)
        qt_spec = qt_match.group('spec')

        make_qt = lambda z, x, y: u.to_quadindex(z, x, y, qt_spec)
        replacements[qt_tag] = '%(qt)s'

    fmtstr = reduce(lambda s, (old, new): new.join(s.split(old)), replacements.iteritems(), template)
    def _url(z, x, y):
        if shards:
            shard = shards[(x + y) % len(shards)]
        if make_qt:
            qt = make_qt(z, x, y)
        inv_y = 2**z - 1 - y
        return fmtstr % locals()
    return _url

def query_tiles(sess, layer, chunk, refresh_cutoff, refresh_cutoff_missing):
    """see existing_tiles; query the set of tiles 'chunk' to see which already exist.
    'cutoff's are timestamps instead of intervals now"""
    q = sess.query(mt.Tile).filter_by(layer=layer).filter(tuple_(mt.Tile.z, mt.Tile.x, mt.Tile.y).in_(list(chunk)))
    def cutoff_criteria():
        if refresh_cutoff is not None:
            yield and_(mt.Tile.uuid != null_digest(), mt.Tile.fetched_on > refresh_cutoff)
        if refresh_cutoff_missing is not None:
            yield and_(mt.Tile.uuid == null_digest(), mt.Tile.fetched_on > refresh_cutoff_missing)
    coc = list(cutoff_criteria())
    if coc:
        q = q.filter(or_(*coc))
    return set((layer, t.z, t.x, t.y) for t in q)

def find_existing_tiles(sess, tiles, layer, refresh_window=None, refresh_window_missing=None, chunk_size=CULL_RESOLUTION):
    """generator that returns which tiles in the set 'tiles' already exist. if a
    'refresh_window' is defined, only tiles fetched within that days (e.g., 7 days)
    are considered to exist.

    refresh_window -- lookback window for tiles with actual data
    refresh_window_missing -- lookback window for tiles that were missing in the map layer
    """

    def cutoff(threshold):
        return datetime.now() - threshold if threshold is not None else None

    refresh_cutoff = cutoff(refresh_window)
    refresh_cutoff_missing = cutoff(refresh_window_missing)

    for chunk in u.chunker(tiles, chunk_size):
        yield (query_tiles(sess, layer, chunk, refresh_cutoff, refresh_cutoff_missing), len(chunk))

def random_walk_level(tiles, window=10):
    """iterate through the tiles for a given zoom level in a random-walky
    fashion, to make it less obvious that tiles are being ripped by a
    script"""

    def xy(t):
        return t[2:]

    target = None
    while tiles:
        if not target:
            # pick a random starting point
            target = u.rand_elem(tiles)
        else:
            # pick the as-yet-unvisited tile closest to the previous active point
            metric = lambda t: u.manhattan_dist(xy(target), xy(t))
            closest = min(metric(t) for t in tiles)
            candidates = [t for t in tiles if metric(t) == closest]
            target = random.choice(candidates)

        # determine the current 'screen view' centered around the active point (width 'window'),
        # and download in random order
        (xmin, ymin) = [f - window / 2 for f in xy(target)]
        (xmax, ymax) = [f + window - 1 for f in (xmin, ymin)]

        swatch = list(u.set_filter(tiles, lambda (lyr, z, x, y): x >= xmin and x <= xmax and y >= ymin and y <= ymax))
        random.shuffle(swatch)
        for t in swatch:
            yield t

def random_walk(tiles):
    """iterate through all tiles in a random-walk fashion, but proceeding through
    zoom levels in order (download 'bigger' tiles first)"""
    zooms = sorted(set(z for lyr, z, x, y in tiles))
    for zoom in zooms:
        for t in random_walk_level(u.set_filter(tiles, lambda (lyr, z, x, y): z == zoom)):
            yield t

def register_tile(sess, tile, data, hashfunc):
    """save a tile to disk and to database"""
    tile.save(data, hashfunc, sess=sess)
    commit_tile(sess, tile)

def commit_tile(sess, t):
    existing = sess.query(mt.Tile).get(t.pk())
    old_uuid = None
    if existing:
        # if tile exists, update the existing tile object
        if existing.uuid != t.uuid:
            old_uuid = existing.uuid
            existing.uuid = t.uuid
    else:
        sess.add(t)

    # if updated existing tile, possibly delete the old tile image data
    if old_uuid and old_uuid != null_digest():
        if not sess.query(mt.Tile).filter_by(uuid=old_uuid).count():
                # no tile references this file uuid anymore; delete
                mt.TileData(uuid=old_uuid).remove(sess)

    sess.commit()

def digest(data):
    if data is not None:
        return hashlib.sha1(data).hexdigest()[:HASH_LENGTH*2]
    else:
        return null_digest()

def normdata(status, data):
    # treat '302 FOUND' as not found because we assume any redirect is to a generic 'missing' tile
    # no competent tile server would use redirects for normal tiles
    not_found = (status != httplib.OK)
    return data if not not_found else None

def _process_tile(sess, tile, status, data):
    """process a tile download result, accounting for some common errors"""

    if status in (httplib.OK, httplib.NOT_FOUND, httplib.FOUND):
        try:
            register_tile(sess, tile, normdata(status, data), digest)
        except IOError:
            raise Exception('%s: could not write file' % str(tile.pk()))
    else:
        if status == None:
            raise Exception('Tile %s: download error %s' % (str(tile.pk()), data))
        elif status == httplib.FORBIDDEN:
            raise Exception('Warning: we may have been banned')
        else:
            raise Exception('Unrecognized response code %d (tile %s)' % (status, str(tile.pk())))

def process_tile(sess, tile, status, data):
    try:
        _process_tile(sess, tile, status, data)
        return (True, None)
    except Exception, e:
        return (False, str(e))

def tile_counts(tiles, max_depth=None):
    """determine how many tiles to be downloaded at each zoom level"""
    totals = collections.defaultdict(lambda: 0, u.map_reduce(tiles, lambda (layer, z, x, y): [(z,)], len))
    max_tile_zoom = max(totals.keys()) if totals else -1
    max_depth = max_depth if max_depth is not None else -1
    return [totals[z] for z in range(max(max_tile_zoom, max_depth) + 1)]

# monitor threads below must have a function:
#   status() => (# processed, total # to process, # error occurred thus far)

class TileEnumerator(threading.Thread):
    """a monitorable thread to enumerate all tiles in a download region"""

    def __init__(self, region, layers):
        threading.Thread.__init__(self)

        def mk_tess(layer, depth):
            return mt.RegionTessellation(region, depth, min_zoom=u.layer_property(layer, 'min_depth', 0))
        self.tesss = dict((L['name'], mk_tess(L['name'], L['zoom'])) for L in layers)
        self.est_num_tiles = sum(tess.size_estimate() for tess in self.tesss.values())
        self.tiles = set()
        self.count = 0

    def run(self):
        for layer, tess in self.tesss.iteritems():
            for t in tess:
                z, x, y = t
                self.tiles.add((layer, z, x, y))
                self.count += 1
        self.est_num_tiles = self.count

    def status(self):
        return (self.count, self.est_num_tiles, 0)

class TileCuller(threading.Thread):
    """a monitorable thread to enumerate which tiles must be downloaded (i.e., do not
    already exist)"""

    def __init__(self, tiles, layers, sess):
        threading.Thread.__init__(self)

        self.tiles = tiles
        self.layers = dict((L['name'], L) for L in layers)
        self.sess = sess

        self.num_tiles = len(tiles)
        self.num_processed = 0

    def run(self):
        tiles_by_layer = u.map_reduce(self.tiles, lambda (layer, z, x, y): [(layer, (z, x, y))])
        for layername, layer_tiles in tiles_by_layer.iteritems():
            self.cull_layer(self.layers[layername], layer_tiles)

    def cull_layer(self, layer, tiles):
        refr_window = timedelta(days=layer['refr']) if layer['refr'] is not None else None
        refresh_window = refresh_window_missing = refr_window

        if refresh_window == timedelta(0) and refresh_window_missing == timedelta(0):
            # we must (re-)download all; don't bother checking existing
            existing_tile_stream = None
        else:
            existing_tile_stream = find_existing_tiles(self.sess, tiles, layer['name'], refresh_window, refresh_window_missing)

        existing_tiles = set()
        if existing_tile_stream:
            for existing, num_queried in existing_tile_stream:
                existing_tiles |= existing
                self.num_processed += num_queried
        else:
            self.num_processed += len(tiles)

        self.tiles -= existing_tiles

    def status(self):
        return (self.num_processed, self.num_tiles, 0)

class TileDownloader(threading.Thread):
    """a monitorable thread that downloads and processes tiles"""

    def __init__(self, tiles, sess):
        threading.Thread.__init__(self)

        self.tiles = tiles
        self.num_tiles = len(tiles)

        # error count and last error are displayed in the curses interface
        self.error_count = 0
        self.last_error = None

        self.dlmgr = DownloadManager(limit=100)

        def process(key, status, data):
            return process_tile(sess, key, status, data)
        self.dlpxr = DownloadProcessor(self.dlmgr, process, self.num_tiles, self.onerror)

    def run(self):
        self.dlmgr.start()
        self.dlpxr.start()

        for t in random_walk(self.tiles):
            layer, z, x, y = t
            tile = mt.Tile(layer=layer, z=z, x=x, y=y)
            self.dlmgr.enqueue((tile, tile.url()))

        self.dlpxr.join()
        self.dlmgr.terminate()
        self.dlmgr.join()

    def onerror(self, msg):
        self.error_count += 1
        self.last_error = msg

    def status(self):
        return (self.dlpxr.count, self.num_tiles, self.error_count)

class DownloadProcessor(threading.Thread):
    """thread that consumed the download output queue and processes the resultant tile data"""

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

class DownloadService(object):

    def __init__(self, process, sess=None):
        self.sess = sess
        def _process(meta, status, data):
            self.cache_tile(meta['tile'], status, data, meta.get('cache', False), meta.get('overwrite', False))
            process(meta, status, data)
            return (True, None)

        self.dlmgr = DownloadManager(limit=100)
        self.dlpxr = DownloadProcessor(self.dlmgr, _process)
        self.started = False

    def add(self, key, url):
        if not self.started:
            self.start()
        self.dlmgr.enqueue((key, url))

    def cache_tile(self, t, status, data, cache, overwrite):
        if cache:
            if overwrite or not self.sess.query(mt.Tile).get(t.pk()):
                try:
                    _process_tile(self.sess, t, status, data)
                except:
                    pass

    def start(self):
        self.started = True
        self.dlmgr.start()
        self.dlpxr.start()

    def terminate(self):
        self.dlmgr.terminate()
        self.dlpxr.terminate()
