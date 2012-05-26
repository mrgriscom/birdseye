import logging
import logging.handlers

### DATABASES AND DIRECTORIES

# database connector for tile info
TILE_DB = 'postgresql:///tiles'

# if true, store tile images in database as BLOBs
# if false, store as files in TILE_ROOT
TILE_STORE_BLOB = False

# root directory where tiles are stored
TILE_ROOT = '/var/lib/birdseye/tiles/'
# how to clump tiles into directory buckets (shouldn't have too many
# entries in any one directory)
# [2, 4]: 53392f0a.jpg => 53/5339/53392f0a.jpg
TILE_BUCKETS = [3]

# database connector for tracklog
GPS_LOG_DB = 'postgresql:///geoloc'


### GPS CONFIGURATION

# gps device
GPS_DEVICE = '/dev/ttyUSB0'

# gps device data rate
BAUD_RATE = 57600

# two reports from the gps will be considered part of the same
# sample if their timestamps differ by less than this amount
GPS_SAMPLE_WINDOW = 0.1 #seconds

# how long to wait for all of a sample's data to come in from
# gps, before dispatching sample and ignoring late-arriving
# data
GPS_BUFFER_WINDOW = 0.3 #seconds

# class that devices custom behavior for gps device
GPS_DEVICE_POLICY = 'gps.gpslistener.BU353DevicePolicy'


### MAP LAYERS AND CACHING

LAYERS = {
    'layername': {
        'tile_url': 'http://mapserver/tile?x={x}&y={y}&z={z}',
        # tile_url may also be a function, called once the first time this
        # layer is accessed, returning either:
        #   - a template string
        #   - another function [(z, x, y) => url template str] to be called
        #     for every tile access
        'file_type': 'png',
        'name': 'sample layer',

        # optional
        'cacheable': True,
        'overlay': False,
        'min_depth': 0,
    },


    'osmmapnik': {
        'tile_url': 'http://{s:abc}.tile.openstreetmap.org/{z}/{x}/{y}.png',
        'file_type': 'png',
        'name': 'openstreetmap standard (mapnik)',
    },

    'bingsatlab': {
        'tile_url': 'http://ecn.dynamic.t{s:0-3}.tiles.virtualearth.net/comp/CompositionHandler/{qt}?it=A,G,L&n=z',
        'file_type': 'jpg',
        'name': 'bing satellite labelled',
        'min_depth': 1,
    },

    'chartbundle': {
        'tile_url': 'http://wms.chartbundle.com/tms/1.0.0/sec/{z}/{x}/{-y}.{type}',
        'file_type': 'png',
        'name': 'faa aeronautical (vfr sectional)',
    },
}

TILE_DL_UA = 'Mozilla/5.0 (X11; U; Linux i686; en-US) Gecko/20080208 Firefox/2.0.0.13'


### MAP RENDERING AND NAVIGATION

# measurement units
UNITS = 'us' # 'us' or 'metric'

WAYPOINTS = '~/.birdseye/waypoints'










# logging config
LOGFILE = '/tmp/birdseye.log'
def init_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.handlers.RotatingFileHandler(LOGFILE, maxBytes=2**24, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
    root.addHandler(handler)



try:
    from localsettings import *
except ImportError:
    pass
