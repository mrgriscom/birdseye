import sys
import yaml
import re
from datetime import datetime, timedelta
from mapcache import mapdownload
import settings
import curses
from mapcache import maptile as mt
import math
import time
import logging
import util.util as u

from sqlalchemy.exc import InvalidRequestError

## INPUT VALIDATION

def parse_yaml_args(f):
    try:
        data = yaml.safe_load(f)
    except yaml.YAMLError:
        raise RuntimeError('cannot parse directives; exiting...')

    args = {}

    args['name'] = str(data.get('name', ''))

    args['update'] = bool(data.get('update'))
    if args['update'] and args['name'] == mt.Region.GLOBAL_NAME:
        raise RuntimeError('"%s" region is read-only' % mt.Region.GLOBAL_NAME)

    args['region'] = validate_region(str(data['region'])) if 'region' in data else None

    args['layers'] = [parse_yaml_layer(str(name), info) for name, info in data['layers'].iteritems()]
    for layer in args['layers']:
        validate_layer(layer)

    return args

def parse_yaml_layer(layername, data):
    lyr = {'name': layername}

    if 'zoom' not in data:
        raise RuntimeError('layer zoom level required')
    lyr['zoom'] = int(data['zoom'])
    if lyr['zoom'] < 0 or lyr['zoom'] > 30:
        fatal('zoom level outside allowed range')

    refresh_window = data.get('refresh-older-than') # days
    if refresh_window is not None:
        lyr['refr'] = float(refresh_window)
    else:
        refr = data.get('refresh-mode', 'never')
        if refr == 'always':
            lyr['refr'] = 0
        elif refr == 'never':
            lyr['refr'] = None
        else:
            raise RuntimeError('unrecognized refresh mode')

    return lyr

def validate_region(region_def):
    if region_def == mt.Region.GLOBAL_NAME:
        return mt.Region.world()

    pcs = [pc for pc in re.split('(\s+|,)', region_def) if pc.strip()]
    if len(pcs) % 3 != 0:
        raise RuntimeError('can\'t parse region boundary')
    by_pos = [[pcs[i] for i in range(k, len(pcs), 3)] for k in range(0, 3)]
    if set(by_pos[1]) != set(','):
        raise RuntimeError('can\'t parse region boundary')
    try:
        (lats, lons) = [[float(f) for f in by_pos[k]] for k in (0, 2)]
    except ValueError:
        raise RuntimeError('can\'t parse region boundary')

    coords = zip(lats, lons)
    if len(coords) < 3:
        raise RuntimeError('region must have at least 3 coordinates')

    for (lat, lon) in coords:
        if abs(lat) > 90. or abs(lon) > 180.:
            raise RuntimeError('region coordinates out of range')

    return mt.Region('__', coords)

def validate_layer(layer):
    layername = layer['name']

    if not layername in settings.LAYERS:
        raise RuntimeError('unrecognized layer "%s"' % layername)
    if not u.layer_property(layername, 'cacheable', True):
        raise RuntimeError('layer "%s" is flagged as non-downloadable' % layername)

def db_validate(args):
    sess = mt.dbsess()

    try:
        region = sess.query(mt.Region).filter_by(name=args['name']).one()
        if args['region']:
            if args['update']:
                region.boundary = args['region'].boundary
                sess.commit()
            else:
                if (args['region'].poly() ^ region.poly()).area() > 1e-6:
                    raise RuntimeError('region "%s" already exists (set "update" flag?)' % args['name'])

    except InvalidRequestError:
        if not args['region']:
            raise RuntimeError('region "%s" does not exist (supply a region boundary?)' % args['name'])

        if args['name']:
            args['region'].name = args['name']
            sess.add(args['region'])
            sess.commit()
        region = args['region']

    args['region'] = region.merc_poly()


## DOWNLOADER INTERFACE

def download(poly, layers):
    curses.wrapper(download_curses, poly, layers)

def download_curses(w, polygon, layers):
    te = mapdownload.TileEnumerator(polygon, layers)
    monitor(w, 0, te, 'Enumerating', 15, 3)

    print_tile_counts(w, mapdownload.tile_counts(te.tiles), 'Tiles in region', 4, 2)

    tc = mapdownload.TileCuller(te.tiles, layers, mt.dbsess())
    monitor(w, 1, tc, 'Culling', 15)

    print_tile_counts(w, mapdownload.tile_counts(tc.tiles, max(L['zoom'] for L in layers)), 'Tiles to download', 4, 19)

    td = mapdownload.TileDownloader(tc.tiles, mt.dbsess())
    monitor(w, 2, td, 'Downloading', 15, erry=3)

    try:
        while True:
            time.sleep(.01)
    except KeyboardInterrupt:
        pass




def monitor(w, y, thread, caption, width, sf=0, erry=None):
    thread.start()
    thread.start_at = time.time()
    while thread.isAlive():
        update_status(w, thread, False, y, caption, width, sf, erry)
        time.sleep(.01)
    update_status(w, thread, True, y, caption, width, sf, erry)

def update_status(w, thread, done, y, caption, width, sf, erry):
    println(w, status(caption, thread.status(), thread.start_at, width, sf if not done else 0), y)
 
    if erry != None:
        err = thread.last_error
        if err:
            println(w, err, erry, 2)

def println(w, str, y, x=0):
    w.addstr(y, x, str)
    w.clrtoeol()
    w.refresh()

def status(caption, (k, n, e), start_at, width=None, est_sigfig=0):
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

    elapsed = u.format_interval(time.time() - start_at, colons=True, max_unit='h')

    pad = len(str(n))
    errstr = '   [Errors: %4d]' % e if e > 0 else ''
    return '%s %s%6.2f%% [%*d/%s] [%s] %s' % ((caption + ':').ljust(width + 1), '+' if overflow else ' ',
            100. * ratio, pad, k, max_str, elapsed, errstr)

def print_tile_counts(w, counts, header, y, x, width=None, zheader='Zoom'):
    if not width:
        width = max(len(header), 10)
    zwidth = len(zheader)

    w.addstr(y, 0, zheader)
    w.addnstr(y, zwidth + x, header.rjust(width), width)
    for i in range(0, len(counts)):
        w.addstr(y + 1 + i, 0, '%*d' % (zwidth, i))
        w.addstr(y + 1 + i, zwidth + x, '%*d' % (width, counts[i] if i < len(counts) else 0))

    w.refresh()



def fatal(msg):
    sys.stderr.write(msg + '\n')
    sys.exit()

if __name__ == "__main__":
    settings.init_logging()

    u.setup()

    try:
        specfile = open(sys.argv[1])
    except IndexError:
        specfile = sys.stdin

    try:
        args = parse_yaml_args(specfile)
        db_validate(args)
    except RuntimeError, e:
        fatal(str(e))

    download(args['region'], args['layers'])




