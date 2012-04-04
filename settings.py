import logging

# database connector for tile info
TILE_DB = 'postgresql:///navdata'

# root directory where tiles are stored
TILE_ROOT = '/home/drew/tiles/'

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

# database connector for tracklog
GPS_LOG_DB = 'postgresql:///geoloc'

# measurement units
UNITS = 'us' # 'us' or 'metric'

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
