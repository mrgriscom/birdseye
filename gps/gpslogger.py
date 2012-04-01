import gpslistener
import logging
import threading
import time
import zmq
import settings

from sqlalchemy import create_engine, Column, DateTime, Float, String, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

fields = [
    ('gps_time', {'type': DateTime, 'args': {'primary_key': True}, 'field': 'time'}),
    ('system_time', {'type': DateTime, 'args': {'nullable': False}, 'field': 'systime'}),
    ('latitude', {'type': Float, 'min': -90., 'max': 90., 'args': {'nullable': False}, 'field': 'lat'}),
    ('longitude', {'type': Float, 'min': -180., 'max': 180., 'args': {'nullable': False}, 'field': 'lon'}),
    ('altitude', {'type': Float, 'min': -1000., 'max': 100000., 'field': 'alt'}),
    ('speed', {'type': Float, 'min': 0., 'max': 1000.}),
    ('heading', {'type': Float, 'min': 0., 'max': 360., 'closed': True}),
    ('climb', {'type': Float, 'min': -1000., 'max': 1000.}),
    ('h_error', {'type': Float, 'min': 0., 'max': 5000.}),
    ('v_error', {'type': Float, 'min': 0., 'max': 5000.}),
    ('fix_type', {'type': String}),
    ('comment', {'type': String}),
]

Base = declarative_base()
class Fix(Base):
    __tablename__ = 'gps_log'

    gps_time = Column(DateTime, primary_key=True)

    def __init__(self, data):
        data['comment'] = ','.join(data['comments']) if data['comments'] else None
        for field, config in fields:
            setattr(self, field, data[config.get('field') or field])

for field, config in fields:
    if config.get('args', {}).get('primary_key'):
        continue

    args = [config['type']]
    if config.get('min') is not None:
        args.append(CheckConstraint('%s >= %s and %s %s %s' % (field, config['min'], field, '<' if config.get('closed') else '<=', config['max'])))
    col = Column(*args, **config.get('args', {}))
    setattr(Fix, field, col)



class GPSLogger(threading.Thread):
    MAX_BUFFER = 60        # fixes
    COMMIT_INTERVAL = 180  # seconds
    DISPATCH_RETRY_WAIT = 3.

    def __init__(self, dbconnector):
        threading.Thread.__init__(self)
        self.up = True

        self.engine = create_engine(dbconnector)
        self.dbsess = None
        self.gps_acquire = None
        self.gps = None
        self.buffer = []
        self.buffer_age = None

        Base.metadata.create_all(self.engine)

    def terminate (self):
        self.up = False

    def run (self):
        try:
            self.dbsess = sessionmaker(bind=self.engine)()
        except:
            logging.exception('gpslogger can\'t connect to db')
            return

        self.gps_acquire = gpslistener.GPSSubscriber(self.DISPATCH_RETRY_WAIT)
        self.gps = self.gps_acquire.acquire(lambda: not self.up)
        if not self.gps:
            return

        while self.up:
            try:
                data = self.gps.get_fix()
                if data != None:
                    self.process_fix(data)

                if self.flush_due():
                    self.flush()
            except zmq.ZMQError:
                logging.warn('gpslogger: broken connection; exiting...')
                self.terminate()
            except:
                logging.exception('error in main logger loop')

        self.flush()
        self.gps.unsubscribe()
        self.dbsess.close()

    def process_fix(self, data):
        self.buffer.append(data)
        if self.buffer_age is None:
            self.buffer_age = time.time()

    def buffer_size(self):
        return len(self.buffer)

    def flush_due(self):
        if self.buffer_age and time.time() - self.buffer_age > self.COMMIT_INTERVAL:
            return True
        elif self.buffer_size() >= self.MAX_BUFFER:
            return True

    def flush(self):
        for fix in self.buffer:
            try:
                self.dbsess.add(Fix(fix))
                self.dbsess.commit()
            except:
                logging.exception('error committing fix: %s' % fix)
                self.dbsess.rollback()
        self.buffer = []
        self.buffer_age = None

